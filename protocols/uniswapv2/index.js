// require("log-timestamp")(function () {
//   return new Date().toISOString() + " %s";
// });
const fs = require("fs");
const _colors = require("colors");
const moment = require("moment");
const schedule = require("node-schedule");
const cliProgress = require("cli-progress");
const factoryABI = JSON.parse(fs.readFileSync("./abi/uniswapFactoryABI.json"));
const poolABI = JSON.parse(fs.readFileSync("./abi/uniswapPoolABI.json"));
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
let factoryContract;
let totalPools = 0;
let msgSequence = 0;

let streamQueue = require("fastq").promise(streamWorker, 1);

const lazy_load_map = new Map(); // Used when live
const pool_pairs_map = new Map(); // Used when live
const tokens_map = new Map(); // Used all the time to minimize request to static data
const nonStandartToken = new Map(); // Used all time time since we have a couple non standart erc20

const UNISWAPV2_FACTORY = "0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f"; // mainnet and testnet

async function init(web3Obj) {
  web3 = web3Obj;
  factoryContract = new web3.eth.Contract(factoryABI, UNISWAPV2_FACTORY);

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

  // await populateSwapPools(0);
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
    console.error("[UNISWAPV2]", tokens_map.size, "pre loaded tokens");
  }

  console.log("[UNISWAPV2] Loading Pools and Pairs");
  const pools = await loadListFromMemory("POOLS:ETHEREUM:UNISWAPV2");

  if (!pools) {
    console.error("[UNISWAPV2] No pools in memory, loading it...");
    await populateSwapPools(0);
    console.error(
      "[UNISWAPV2] All pools are loaded, exiting and starting again..."
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
        console.error("[UNISWAPV2] loadMaps", error.name + ":" + error.message);
        process.exit();
      }
    });
  };

  if (pools) {
    await loadMaps();
  }

  swapWatcher();

  console.log("[UNISWAPV2] Started", totalPools, "pools");

  // Lazy Load
  await populateSwapPools(0);

  // Shedulle pools check for every minute
  schedule.scheduleJob("* * * * *", function () {
    console.log("[UNISWAPV2] Checking for new pools");
    factoryContract.methods
      .allPairsLength()
      .call()
      .then((allPairsLength) => {
        if (totalPools != allPairsLength) {
          const difference = allPairsLength - totalPools;
          if (difference < 0) {
            console.error(
              "[UNISWAPV2] UNISWAPV2 POOLS ARE INCONSISTENT:",
              difference
            );
          } else {
            console.log(
              "[UNISWAPV2] New pools found:",
              difference,
              " starting from:",
              totalPools
            );
            populateSwapPools(totalPools).then(() => {
              console.log(
                "[UNISWAPV2] New pools updated, new total is:" + totalPools
              );
            });
          }
        } else {
          console.log("[UNISWAPV2] All pools are update:", totalPools);
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
        chain: "ETHEREUM",
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
    const poolContract = new web3.eth.Contract(poolABI, poolID);
    let token0Addr = await poolContract.methods.token0().call();
    token0Addr = token0Addr.toLowerCase();
    let token1Addr = await poolContract.methods.token1().call();
    token1Addr = token1Addr.toLowerCase();
    const symbol = await poolContract.methods.symbol().call();
    const name = await poolContract.methods.name().call();
    const decimals = await poolContract.methods.decimals().call();
    const swap_fee = "0.30";

    let tokens = [];

    const reserves = await poolContract.methods.getReserves().call();

    const token0 = await getTokenData(token0Addr);
    if (token0 === null) {
      fs.appendFileSync("tokens0.txt", token0Addr + "\n");
      console.error("[UNISWAPV2] Token not found:", token0Addr);
      return;
    }

    const token1 = await getTokenData(token1Addr);
    if (token1 === null) {
      fs.appendFileSync("tokens1.txt", token1Addr + "\n");
      console.error("[UNISWAPV2] Token not found:", token1Addr);
      return;
    }

    token0.weight = "50";
    token0.reserves = Utils.toTokenUnitsBN(
      reserves[0],
      token0.decimals
    ).toString();
    token1.weight = "50";
    token1.reserves = Utils.toTokenUnitsBN(
      reserves[1],
      token1.decimals
    ).toString();
    tokens = [token0, token1];

    return {
      chain: "ETHEREUM",
      protocol: "UNISWAPV2",
      pool_id: poolID,
      symbol: symbol,
      name: name,
      swap_fee: swap_fee,
      decimals: decimals.toString(),
      immutable: true,
      tokens: tokens,
    };
  } catch (error) {
    console.error("[UNISWAPV2] getPoolData", error.name + ":" + error.message);
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
      "[UNISWAPV2] loadListFromMemory",
      error.name + ":" + error.message
    );
  }
};

async function populateSwapPools(beginPoolIndex) {
  try {
    // create new progress bar
    const bar1 = new cliProgress.SingleBar({
      format:
        "[UNISWAPV2] Loading Pools |" +
        _colors.cyan("{bar}") +
        "| {percentage}% || {value}/{total} Pools",
      barCompleteChar: "\u2588",
      barIncompleteChar: "\u2591",
      hideCursor: true,
    });

    const allPairsLength = await factoryContract.methods
      .allPairsLength()
      .call();

    if (allPairsLength.length == 0) {
      return false;
    }

    bar1.start(allPairsLength, beginPoolIndex);

    for (var i = beginPoolIndex; i < allPairsLength; i++) {
      let poolID = await factoryContract.methods.allPairs(i).call();
      poolID = poolID.toLowerCase();

      // The socket is ahead, so no need to load it
      if (lazy_load_map.has(poolID)) {
        continue;
      }

      const pair = await getPoolData(poolID);

      if (!pair) {
        console.info("[UNISWAPV2] Problem creating a pair for pool", poolID);
        continue;
      }

      await redisClient.hset(
        "POOLS:ETHEREUM:UNISWAPV2",
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

      bar1.update(i);
    }

    // process finished
    bar1.update(allPairsLength);

    totalPools = allPairsLength;
    return allPairsLength;
  } catch (error) {
    console.error(
      "[UNISWAPV2] populateSwapPools",
      error.name + ":" + error.message
    );
  }
}

const createNewPool = async (poolID) => {
  // We expect to know beforehand all pools that belongs to uni swap, this could be a false
  // positive while loading all pools on populateSwapPools only, but should fix itself
  if (!pool_pairs_map.has(poolID)) {
    console.info("[UNISWAPV2] Not a uniswap poll", poolID + "\n");
    return false;
  }

  const pair = await getPoolData(poolID);
  if (!pair) {
    bancor_not_valid.set(poolID, true);
    console.info("[UNISWAPV2] Problem creating a pair for pool", poolID + "\n");
    return false;
  }

  if (!pool_pairs_map.has(poolID)) {
    totalPools++;
  }

  pool_pairs_map.set(poolID, pair);

  await redisClient.hset(
    "POOLS:ETHEREUM:UNISWAPV2",
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
        "TICKERS_ETHEREUM_UNISWAPV2",
        poolID,
        JSON.stringify(ticker)
      );

      await redisClient.hset(
        "POOLS:ETHEREUM:UNISWAPV2",
        poolID,
        JSON.stringify(pair)
      );

      await redisClient.set(
        "ACTIVE:ETHEREUM:UNISWAPV2:" + poolID,
        JSON.stringify(pair)
      );
      await redisClient.expire("ACTIVE:ETHEREUM:UNISWAPV2:" + poolID, 86400);

      msgSequence++;
    } else {
      // console.log("POOL NOT FOUND ", sync.address.toLowerCase());
      // newPoolsQueue.push(sync.address.toLowerCase());
    }
    return true;
  } catch (error) {
    console.error("[UNISWAPV2] streamWorker", error.name + ":" + error.message);
  }
}
const swapWatcher = async () => {
  chainId = await web3.eth.getChainId();

  web3.eth
    .subscribe("logs", {
      topics: [
        "0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1", // SYNC
      ],
    })
    .on("connected", async () => {
      console.log(
        "[UNISWAPV2] Connected to Ethereum: Listening for syncs on CHAIN ID:" +
          chainId
      );
    })
    .on("data", async (sync) => {
      streamQueue.push(sync);
    })
    .on("error", async (error) => {
      console.log("[UNISWAPV2]" + error);
    });
};

module.exports = {
  init,
};
