// require("log-timestamp")(function () {
//   return new Date().toISOString() + " %s";
// });
const fs = require("fs");
const Decimal = require("decimal.js");
const _colors = require("colors");
const moment = require("moment");
const schedule = require("node-schedule");
const factoryABI = JSON.parse(fs.readFileSync("./abi/balancerFactoryABI.json"));
const poolABI = JSON.parse(fs.readFileSync("./abi/balancerPoolABI.json"));
const ERC20ABI = JSON.parse(fs.readFileSync("./abi/ERC20ABI.json"));
const ERC20Bytes32ABI = JSON.parse(
  fs.readFileSync("./abi/ERC20Bytes32ABI.json")
);
// const MOCKDATA = JSON.parse(fs.readFileSync("./protocols/balancer/mock.json"));
const { nonStandartERC20ABI } = require("../../tokens/no_standarized_erc20");
const Redis = require("../../libs/redis");
const ZMQ = require("../../libs/zmq");

let web3;
let redisClient;
let factoryContract;
let totalPools = 0;
let msgSequence = 0;

// Hardcoded only for balancer
let fromBlock = 9569110;
let toBlock = 9569113 + 10000;
let latestBlocKnown = 0;
let latestBlocKChecked = fromBlock;

let streamQueue = require("fastq").promise(streamWorker, 1);

const lazy_load_map = new Map(); // Used when live

const not_balancer_map = new Map(); // Used when live to not fload the logs with same info
const pool_pairs_map = new Map(); // Used when live
const tokens_map = new Map(); // Used all the time to minimize request to static data
const nonStandartToken = new Map(); // Used all time time since we have a couple non standart erc20

const pools_loading = new Map(); // Used only when loading historical data of balancer

const BALANCER_FACTORY = "0x9424B1412450D0f8Fc2255FAf6046b98213B76Bd"; // mainnet and testnet

const _bpool_code_hash =
  "3cec846b68239958db567c515d08c6b451c9317d08c074c0ba15557d4997067a";

const LOG_SWAP =
  "0x908fb5ee8f16c6bc9bc3690973819f32a4d4b10188134543c88706e0e1d43378";
const LOG_JOIN =
  "0x63982df10efd8dfaaaa0fcc7f50b2d93b7cba26ccc48adee2873220d485dc39a";
const LOG_EXIT =
  "0xe74c91552b64c2e2e7bd255639e004e693bd3e1d01cc33e65610b86afcc1ffed";
const LOG_NEW_POOL =
  "0x8ccec77b0cb63ac2cafd0f5de8cdfadab91ce656d262240ba8a6343bccc5f945";

async function init(web3Obj) {
  web3 = web3Obj;
  factoryContract = new web3.eth.Contract(factoryABI, BALANCER_FACTORY);

  redisClient = await Redis.redisClient();

  const loadNonStandartsTokens = () => {
    return new Promise((resolve) => {
      for (var i = 0; i < nonStandartERC20ABI.length; i++) {
        nonStandartToken.set(
          nonStandartERC20ABI[i].address.toString().toLowerCase(),
          nonStandartERC20ABI[i]
        );
      }
      resolve();
    });
  };

  await loadNonStandartsTokens();

  // const pair = await getPoolData("0x96d99093f22719dd06fb8db8e93779979a2acab3");
  // console.log(pair);
  // process.exit();

  // const pair = await getPoolData("0x7409b0ca346b6f14f3124c850714cd866db0b9c1");
  // console.log(pair);
  // process.exit();

  // const pair = await getPoolData("0xEeb82Fa092e3CEcB1ba99F8342775B70Efb40cF8");
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
    console.error("[BALANCER]", tokens_map.size, "pre loaded tokens");
  }

  console.log("[BALANCER] Loading Pools and Pairs");
  const pools = await loadListFromMemory("POOLS:ETHEREUM:BALANCER");

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
        console.error("[BALANCER loadMaps] ", error.name + ":" + error.message);
        process.exit();
      }
    });
  };

  if (pools) {
    await loadMaps();
  }

  swapWatcher();

  console.log("[BALANCER] Started", totalPools, "pools");

  await populateSwapPools();

  //Shedulle pools check for every minute
  // schedule.scheduleJob("* * * * *", function () {
  //   checkForNewPools();
  // });
}

async function checkForNewPools() {
  if (latestBlocKChecked != latestBlocKnown) {
    console.log("[BALANCER] Checking blocks for logs");

    await populateSwapPools();

    console.info("[BALANCER] All pools are loaded");
  } else {
    fromBlock = latestBlocKChecked;
    toBlock = await web3.eth.getBlockNumber();
    await populateSwapPools();
  }
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
    const poolContract = await new web3.eth.Contract(poolABI, poolID);
    let tokens = [];
    let swap_fee = 0;

    if (!pool_pairs_map.has(poolID)) {
      // We do this to only check once
      if (not_balancer_map.has(poolID)) {
        return;
      }
      const isBPool = await factoryContract.methods.isBPool(poolID).call();
      if (!isBPool) {
        console.info(
          "[BALANCER] Silencing the address",
          poolID,
          "that does not belong to balancer."
        );
        not_balancer_map.set(poolID, true);
        return;
      }
    }

    const name = await poolContract.methods.name().call();
    const symbol = await poolContract.methods.symbol().call();
    const decimals = await poolContract.methods.decimals().call();
    const immutable = await poolContract.methods.isFinalized().call();

    // The finalized state lets users know that the weights, balances, and fees of this pool are immutable
    const currentTokens = await poolContract.methods.getCurrentTokens().call();
    swap_fee = await poolContract.methods.getSwapFee().call();

    for (var i in currentTokens) {
      let token = await getTokenData(currentTokens[i].toLowerCase());
      if (token === null) {
        fs.appendFileSync("tokens.txt", currentTokens[i].toLowerCase() + "\n");
        console.error(
          "[BALANCER] Token not found: ",
          currentTokens[i].toLowerCase()
        );
        continue;
      }

      const weight = await poolContract.methods
        .getNormalizedWeight(currentTokens[i].toLowerCase())
        .call();

      const balance = await poolContract.methods
        .getBalance(currentTokens[i].toLowerCase())
        .call();

      token.weight = ((weight / 1e18) * 100).toString();
      token.reserves = Decimal(balance)
        .dividedBy("1e" + token.decimals)
        .toString();
      tokens.push(token);
    }

    return {
      chain: "ETHEREUM",
      protocol: "BAlANCER",
      pool_id: poolID,
      symbol: symbol,
      name: name,
      swap_fee: ((swap_fee / 1e18) * 100).toString(),
      decimals: decimals.toString(),
      immutable: immutable,
      tokens: tokens,
    };
  } catch (error) {
    console.error(
      "[BALANCER] getPoolData",
      "pool:",
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
      "[BALANCER] loadListFromMemory",
      error.name + ":" + error.message
    );
  }
};

const savePoolToMap = async (block) => {
  return new Promise((resolve) => {
    for (var i = 0; i < block.length; i++) {
      pools_loading.set(
        "0x" + block[i].topics[2].slice(26).toString().toLowerCase(),
        true
      );
    }
    if (block.length > 0) {
      console.info(
        "[BALANCER] Current number of pools to be loaded",
        pools_loading.size
      );
    }
    resolve();
  });
};

const createNewPool = async (poolID) => {
  const pair = await getPoolData(poolID);
  if (!pair) {
    console.info("[BALANCER] Problem creating a pair for pool", poolID);
    return false;
  }

  if (!pool_pairs_map.has(poolID)) {
    totalPools++;
  }

  pool_pairs_map.set(poolID, pair);

  await redisClient.hset(
    "POOLS:ETHEREUM:BALANCER",
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

async function populateSwapPools() {
  latestBlocKnown = await web3.eth.getBlockNumber();
  let runAgain = true;
  if (fromBlock > latestBlocKnown) {
    latestBlocKChecked = latestBlocKnown;
    console.log(
      "[BALANCER] All pools are update for a total of",
      totalPools,
      "pools in the latest block",
      latestBlocKnown
    );
    runAgain = false;
  }

  const block = await web3.eth.getPastLogs({
    fromBlock: fromBlock,
    toBlock: toBlock,
    address: "0x9424b1412450d0f8fc2255faf6046b98213b76bd",
    topics: [LOG_NEW_POOL],
  });

  await savePoolToMap(block);

  fromBlock = toBlock;
  toBlock += 10000;

  if (runAgain) {
    await populateSwapPools();
  } else {
    // All done, lets save it to redis
    if (pools_loading.size > 0) {
      console.info("[BALANCER] Saving pools in memory: ", pools_loading.size);
    }
    for (const entry of pools_loading.entries()) {
      // The socket is ahead, so no need to load it
      if (lazy_load_map.has(entry[0])) {
        continue;
      }
      await createNewPool(entry[0]);
    }

    pools_loading.clear();
  }
}

async function streamWorker(sync) {
  let pair;
  try {
    const poolID = sync.address.toLowerCase();
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

    ZMQ.zmqSendMsg("TICKERS_ETHEREUM_BALANCER", poolID, JSON.stringify(ticker));

    await redisClient.hset(
      "POOLS:ETHEREUM:BALANCER",
      poolID,
      JSON.stringify(pair)
    );

    await redisClient.set(
      "ACTIVE:ETHEREUM:BALANCER:" + poolID,
      JSON.stringify(pair)
    );
    await redisClient.expire("ACTIVE:ETHEREUM:BALANCER:" + poolID, 86400);

    msgSequence++;

    return true;
  } catch (error) {
    console.error("[BALANCER] streamWorker", error.name + ":" + error.message);
  }
}

const swapWatcher = async () => {
  chainId = await web3.eth.getChainId();

  web3.eth
    .subscribe("logs", {
      topics: [
        [null, LOG_SWAP],
        [null, LOG_JOIN],
        [null, LOG_EXIT],
        [null, LOG_NEW_POOL],
      ],
    })
    .on("connected", async () => {
      console.log(
        "[BALANCER] Connected to Ethereum: Listening for syncs on CHAIN ID:" +
          chainId
      );
    })
    .on("data", async (sync) => {
      streamQueue.push(sync);
    })
    .on("error", async (error) => {
      console.log("[BALANCER]:" + error);
    });
};

module.exports = {
  init,
};
