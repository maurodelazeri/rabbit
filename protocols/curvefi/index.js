// require("log-timestamp")(function () {
//   return new Date().toISOString() + " %s";
// });
const fs = require("fs");
const _colors = require("colors");
const moment = require("moment");
const Decimal = require("decimal.js");
const schedule = require("node-schedule");
const addressProviderABI = JSON.parse(
  fs.readFileSync("./abi/curvefiAddressProviderABI.json")
);
const registryABI = JSON.parse(
  fs.readFileSync("./abi/curvefiRegistryABI.json")
);
const poolABI = JSON.parse(fs.readFileSync("./abi/curvefiPoolABI.json"));
const ERC20ABI = JSON.parse(fs.readFileSync("./abi/ERC20ABI.json"));
const ERC20Bytes32ABI = JSON.parse(
  fs.readFileSync("./abi/ERC20Bytes32ABI.json")
);
const { nonStandartERC20ABI } = require("../../tokens/no_standarized_erc20");

const Redis = require("../../libs/redis");
const Utils = require("../../libs/utility");
const ZMQ = require("../../libs/zmq");

let web3;
let redisClient;
let registryContract;
let totalPools = 0;
let msgSequence = 0;

let streamQueue = require("fastq").promise(streamWorker, 1);

const lazy_load_map = new Map(); // Used when live

const pool_pairs_map = new Map(); // Used when live
const tokens_map = new Map(); // Used all the time to minimize request to static data
const nonStandartToken = new Map(); // Used all time time since we have a couple non standart erc20

const CURVEFI_ADRESS_PROVIDER = "0x0000000022D53366457F9d5E68Ec105046FC4383"; // mainnet

//https://curve.readthedocs.io/registry-address-provider.html
//https://curve.readthedocs.io/registry-registry.html#deployment-address

async function init(web3Obj) {
  web3 = web3Obj;
  const addressProvider = new web3.eth.Contract(
    addressProviderABI,
    CURVEFI_ADRESS_PROVIDER
  );
  const registry = await addressProvider.methods.get_registry().call();
  registryContract = new web3.eth.Contract(registryABI, registry);

  redisClient = await Redis.redisClient();

  const loadNonStandartsTokens = () => {
    return new Promise((resolve) => {
      for (var i = 0; i < nonStandartERC20ABI.length; i++) {
        nonStandartToken.set(
          nonStandartERC20ABI[i].address.toLowerCase(),
          nonStandartERC20ABI[i]
        );
      }
      resolve();
    });
  };

  await loadNonStandartsTokens();

  // const pair = await getPoolData("0x4ca9b3063ec5866a4b82e437059d2c43d1be596f");
  // const pair = await getPoolData("0xdebf20617708857ebe4f679508e7b7863a8a8eee");
  // console.log(pair);
  // process.exit();

  // If we already have tokens in memory, lets save some time pre loading it
  const tokens = await loadListFromMemory("TOKENS:ETHEREUM");

  if (tokens) {
    const loadTokens = () => {
      return new Promise((resolve) => {
        for (const [key, value] of Object.entries(tokens)) {
          const token = JSON.parse(value);
          tokens_map.set(key, token);
        }
        resolve();
      });
    };
    await loadTokens();
    console.error("[CURVEFI]", tokens_map.size, "pre loaded tokens");
  }

  console.log("[CURVEFI] Loading Pools and Pairs");
  const pools = await loadListFromMemory("POOLS:ETHEREUM:CURVEFI");

  if (!pools) {
    console.error("[CURVEFI] No pools in memory, loading it...");
    await populateSwapPools(0);
    console.error(
      "[CURVEFI] All pools are loaded, exiting and starting again..."
    );
    process.exit();
  }

  const loadMaps = () => {
    return new Promise((resolve) => {
      try {
        for (const [key, value] of Object.entries(pools)) {
          const pair = JSON.parse(value);
          pool_pairs_map.set(key, pair);
          totalPools++;
        }
        resolve();
      } catch (error) {
        console.error("[CURVEFI] loadMaps", error.name + ":" + error.message);
        process.exit();
      }
    });
  };

  if (pools) {
    await loadMaps();
  }

  swapWatcher();

  console.log("[CURVEFI] Started", totalPools, "pools");

  // Lazy Load
  await populateSwapPools(0);

  // Shedulle pools check for every minute
  schedule.scheduleJob("* * * * *", function () {
    console.log("[CURVEFI] Checking for new pools");
    registryContract.methods
      .pool_count()
      .call()
      .then((allPairsLength) => {
        if (totalPools != allPairsLength) {
          const difference = allPairsLength - totalPools;
          if (difference < 0) {
            console.error(
              "[CURVEFI] CURVEFI POOLS ARE INCONSISTENT:",
              difference
            );
          } else {
            console.log(
              "[CURVEFI] New pools found:",
              difference,
              " starting from:",
              totalPools
            );
            populateSwapPools(totalPools).then(() => {
              console.log(
                "[CURVEFI] New pools updated, new total is:" + totalPools
              );
            });
          }
        } else {
          console.log("[CURVEFI] All pools are update:", totalPools);
        }
      });
  });
}

async function getTokenData(address) {
  // ETH case
  if (nonStandartToken.has(address)) {
    const token = nonStandartToken.get(address);
    return token;
  }

  // If this is memory, we do not need to make any extra call
  if (tokens_map.has(address)) {
    const token = tokens_map.get(address);
    return token;
  }
  // ERC20 String return case
  try {
    const token = new web3.eth.Contract(ERC20ABI, address);
    const name = await token.methods.name().call();
    const symbol = await token.methods.symbol().call();
    const decimals = await token.methods.decimals().call();
    return {
      chain: "ETHEREUM",
      address: address,
      name: name,
      symbol: symbol,
      decimals: decimals.toString(),
    };
  } catch (e) {
    // EC20 Bytes32 return case
    try {
      const token = new web3.eth.Contract(ERC20Bytes32ABI, address);
      const name = await web3.utils.toUtf8(await token.methods.name().call());
      const symbol = await web3.utils.toUtf8(
        await token.methods.symbol().call()
      );
      const decimals = await token.methods.decimals().call();
      return {
        address: address,
        name: name,
        symbol: symbol,
        decimals: decimals.toString(),
      };
    } catch (e) {
      return null;
    }
  }
}

const getPoolData = async (poolID) => {
  try {
    const swap_fee =
      ((await registryContract.methods.get_fees(poolID).call())[0] / 1e10) *
      100;

    const lpToken = await registryContract.methods.get_lp_token(poolID).call();
    let lp = await getTokenData(lpToken.toLowerCase());
    if (lp === null) {
      fs.appendFileSync("tokens.txt", lp.toLowerCase() + "\n");
      console.error("[CURVEFI] Token not found: ", lp.toLowerCase());
      return;
    }

    const symbol = lp.symbol;
    const name = lp.name;
    const decimals = lp.decimals.toString();

    let tokens = [];
    const virtual_price = await registryContract.methods
      .get_virtual_price_from_lp_token(lpToken)
      .call();

    const amplification = await registryContract.methods.get_A(poolID).call();

    const balances = await registryContract.methods.get_balances(poolID).call();

    const coins = await registryContract.methods.get_coins(poolID).call();

    let totalTokens = 0;
    for (var i in coins) {
      if (coins[i].toString() == "0x0000000000000000000000000000000000000000") {
        continue;
      }
      totalTokens++;
    }
    for (var i in coins) {
      if (coins[i].toString() == "0x0000000000000000000000000000000000000000") {
        continue;
      }
      let token = await getTokenData(coins[i].toLowerCase());
      if (token === null) {
        fs.appendFileSync("tokens.txt", coins[i].toLowerCase() + "\n");
        console.error("[CURVEFI] Token not found: ", coins[i].toLowerCase());
        return;
      }

      token.reserves = Decimal(balances[i])
        .dividedBy("1e" + token.decimals)
        .toString();

      token.weight = (1 / totalTokens).toString();

      tokens.push(token);
    }

    return {
      chain: "ETHEREUM",
      protocol: "CURVEFI",
      pool_id: poolID,
      symbol: symbol,
      name: name,
      swap_fee: swap_fee.toString(),
      decimals: decimals.toString(),
      amplification: amplification.toString(),
      virtual_price: (virtual_price / 1e18).toString(),
      immutable: false,
      tokens: tokens,
    };
  } catch (error) {
    console.error(
      "[CURVEFI] getPoolData",
      poolID,
      error.name + ":" + error.message
    );
  }
};

const loadListFromMemory = async (hash) => {
  try {
    return new Promise((resolve, reject) => {
      redisClient.hgetall(hash, (err, value) => {
        if (err) reject(err);
        else resolve(value);
      });
    });
  } catch (error) {
    console.error(
      "[CURVEFI] loadListFromMemory",
      error.name + ":" + error.message
    );
  }
};

async function populateSwapPools(beginPoolIndex) {
  try {
    const allPairsLength = await registryContract.methods.pool_count().call();

    if (allPairsLength.length == 0) {
      return false;
    }

    for (var i = beginPoolIndex; i < allPairsLength; i++) {
      let poolID = await registryContract.methods.pool_list(i).call();
      poolID = poolID.toLowerCase();

      // The socket is ahead, so no need to load it
      if (lazy_load_map.has(poolID)) {
        continue;
      }

      const pair = await getPoolData(poolID);

      if (!pair) {
        console.info("[CURVEFI] Problem creating a pair for pool", poolID);
        continue;
      }

      await redisClient.hset(
        "POOLS:ETHEREUM:CURVEFI",
        poolID,
        JSON.stringify(pair)
      );

      if (pair.tokens.length > 1) {
        if (
          !tokens_map.has(pair.tokens[0].address) ||
          !tokens_map.has(pair.tokens[1].address)
        ) {
          const clone = JSON.parse(JSON.stringify(pair));

          let token0 = clone.tokens[0];
          delete token0.weight;
          delete token0.reserves;

          let token1 = clone.tokens[1];
          delete token1.weight;
          delete token1.reserves;

          await redisClient.hset(
            "TOKENS:ETHEREUM",
            clone.tokens[0].address,
            JSON.stringify(token0)
          );
          await redisClient.hset(
            "TOKENS:ETHEREUM",
            clone.tokens[1].address,
            JSON.stringify(token1)
          );
          tokens_map.set(clone.tokens[0].address, clone.tokens[0]);
          tokens_map.set(clone.tokens[1].address, clone.tokens[1]);
        }
      }
    }

    // process finished
    console.info("[CURVEFI]", allPairsLength, "pairs loaded");
    totalPools = allPairsLength;
    return allPairsLength;
  } catch (error) {
    console.error(
      "[CURVEFI] populateSwapPools",
      error.name + ":" + error.message
    );
  }
}

const createNewPool = async (poolID) => {
  // We expect to know beforehand all pools that belongs to curve fi, this could be a false
  // positive while loading all pools on populateSwapPools only, but should fix itself
  if (!pool_pairs_map.has(poolID)) {
    console.info("[CURVEFI] Not a curve poll", poolID + "\n");
    return false;
  }

  const pair = await getPoolData(poolID);
  if (!pair) {
    bancor_not_valid.set(poolID, true);
    console.info("[CURVEFI] Problem creating a pair for pool", poolID + "\n");
    return false;
  }

  if (!pool_pairs_map.has(poolID)) {
    totalPools++;
  }

  pool_pairs_map.set(poolID, pair);

  await redisClient.hset(
    "POOLS:ETHEREUM:CURVEFI",
    poolID,
    JSON.stringify(pair)
  );

  if (pair.tokens.length > 1) {
    if (
      !tokens_map.has(pair.tokens[0].address) ||
      !tokens_map.has(pair.tokens[1].address)
    ) {
      const clone = JSON.parse(JSON.stringify(pair));

      let token0 = clone.tokens[0];
      delete token0.weight;
      delete token0.reserves;

      let token1 = clone.tokens[1];
      delete token1.weight;
      delete token1.reserves;

      await redisClient.hset(
        "TOKENS:ETHEREUM",
        clone.tokens[0].address,
        JSON.stringify(token0)
      );
      await redisClient.hset(
        "TOKENS:ETHEREUM",
        clone.tokens[1].address,
        JSON.stringify(token1)
      );
      tokens_map.set(clone.tokens[0].address, clone.tokens[0]);
      tokens_map.set(clone.tokens[1].address, clone.tokens[1]);
    }
  }
  return true;
};

async function streamWorker(sync) {
  try {
    const poolID = sync.address.toLowerCase();

    if (pool_pairs_map.has(poolID)) {
      // Lazy load, we are indicating that the streaming holds the most recent data
      lazy_load_map.set(poolID, true);

      const success = await createNewPool(poolID);
      if (!success) {
        return;
      }

      pair = pool_pairs_map.get(poolID);

      pair.block_number = sync.blockNumber;
      pair.transaction_hash = sync.transactionHash;
      pair.processed_timestamp = moment().utc().unix();

      const ticker = {
        type: "ticker",
        sequence: msgSequence,
        chain: pair.chain,
        protocol: pair.protocol,
        amplification: pair.amplification,
        virtual_price: pair.virtual_price,
        swap_fee: pair.swap_fee,
        pool_id: poolID,
        block_number: pair.block_number,
        transaction_hash: pair.transaction_hash,
        processed_timestamp: pair.processed_timestamp,
        name: pair.name,
        immutable: pair.immutable,
        symbol: pair.symbol,
        decimals: pair.decimals.toString(),
        tokens: pair.tokens,
      };

      ZMQ.zmqSendMsg(
        "TICKERS_ETHEREUM_CURVEFI",
        poolID,
        JSON.stringify(ticker)
      );

      await redisClient.hset(
        "POOLS:ETHEREUM:CURVEFI",
        poolID,
        JSON.stringify(pair)
      );

      await redisClient.set(
        "ACTIVE:ETHEREUM:CURVEFI:" + poolID,
        JSON.stringify(pair)
      );
      await redisClient.expire("ACTIVE:ETHEREUM:CURVEFI:" + poolID, 86400);

      msgSequence++;
    } else {
      // console.log("POOL NOT FOUND ", sync.address.toLowerCase());
      // newPoolsQueue.push(sync.address.toLowerCase());
    }
    return true;
  } catch (error) {
    console.error("[CURVEFI] streamWorker", error.name + ":" + error.message);
  }
}

const swapWatcher = async () => {
  chainId = await web3.eth.getChainId();

  web3.eth
    .subscribe("logs", {
      topics: [
        "0x8b3e96f2b889fa771c53c981b40daf005f63f637f1869f707052d15a3dd97140", // TokenExchange
      ],
    })
    .on("connected", async () => {
      console.log(
        "[CURVEFI] Connected to Ethereum: Listening for syncs on CHAIN ID:" +
          chainId
      );
    })
    .on("data", async (sync) => {
      streamQueue.push(sync);
    })
    .on("error", async (error) => {
      console.log("[CURVEFI]" + error);
    });
};

module.exports = {
  init,
};
