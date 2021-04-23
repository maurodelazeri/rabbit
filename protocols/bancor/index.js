// require("log-timestamp")(function () {
//   return new Date().toISOString() + " %s";
// });

const fs = require("fs");
const Decimal = require("decimal.js");
const _colors = require("colors");
const moment = require("moment");
const schedule = require("node-schedule");
const factoryABI = JSON.parse(fs.readFileSync("./abi/bancorFactoryABI.json"));
const poolABI = JSON.parse(fs.readFileSync("./abi/bancorPoolABI.json"));
const ERC20ABI = JSON.parse(fs.readFileSync("./abi/ERC20ABI.json"));
const ERC20Bytes32ABI = JSON.parse(
  fs.readFileSync("./abi/ERC20Bytes32ABI.json")
);
const poolTokensABI1 = JSON.parse(
  fs.readFileSync("./abi/bancorPool1Tokens.json")
);
const poolTokensABI2 = JSON.parse(
  fs.readFileSync("./abi/bancorPool2Tokens.json")
);
const { nonStandartERC20ABI } = require("../../tokens/no_standarized_erc20");
const Redis = require("../../libs/redis");
const Utils = require("../../libs/utility");
const ZMQ = require("../../libs/zmq");

const MOCKDATA = JSON.parse(fs.readFileSync("./protocols/bancor/mock.json"));

let web3;
let redisClient;
let factoryContract;
let totalPools = 0;
let msgSequence = 0;

// Hardcoded only for bancor
let fromBlock = 10287430;
let toBlock = 10287430 + 10000;

// let fromBlock = 12111625 - 10000;
// let toBlock = 12111625;

let latestBlocKnown = 0;
let latestBlocKChecked = fromBlock;

// 0x2F9EC37d6CcFFf1caB21733BdaDEdE11c823cCB0
let BANCOR_NETWORK_ADDRESS;

let streamQueue = require("fastq").promise(streamWorker, 1);

const lazy_load_map = new Map(); // Used when live

const bancor_not_valid = new Map(); // Used when live to not fload the logs with same info
const pool_pairs_map = new Map(); // Used when live
const tokens_map = new Map(); // Used all the time to minimize request to static data
const nonStandartToken = new Map(); // Used all time time since we have a couple non standart erc20

const pools_loading = new Map(); // Used only when loading historical data of bancor

const BANCOR_FACTORY = "0x52Ae12ABe5D8BD778BD5397F99cA900624CfADD4"; // mainnet registry

const LOG_CONVERSION =
  "0x7154b38b5dd31bb3122436a96d4e09aba5b323ae1fd580025fab55074334c095";

async function init(web3Obj) {
  web3 = web3Obj;
  factoryContract = new web3.eth.Contract(factoryABI, BANCOR_FACTORY);

  // BancorNetwork
  BANCOR_NETWORK_ADDRESS = await factoryContract.methods
    .addressOf("0x42616e636f724e6574776f726b")
    .call();

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

  // Working
  // const pair = await getPoolData("0x04D0231162b4784b706908c787CE32bD075db9b7");
  // console.log(pair);
  // process.exit();

  // Testing
  // const pair = await getPoolData("0xd2deb679ed81238caef8e0c32257092cecc8888b");
  // console.log(pair);
  // process.exit();

  // const pair = await getPoolData("0x25bf8913d6296a69c7b43bc781614992cb218935");
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
    console.error("[BANCOR]", tokens_map.size, "pre loaded tokens");
  }

  console.log("[BANCOR] Loading Pools and Pairs");
  const pools = await loadListFromMemory("POOLS:ETHEREUM:BANCOR");

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
        console.error("[BANCOR loadMaps] ", error.name + ":" + error.message);
        process.exit();
      }
    });
  };

  if (pools) {
    await loadMaps();
  }

  swapWatcher();

  console.log("[BANCOR] Started", totalPools, "pools");

  await populateSwapPools();

  // Shedulle pools check for every minute
  // schedule.scheduleJob("* * * * *", function () {
  //   checkForNewPools();
  // });
}

async function checkForNewPools() {
  if (latestBlocKChecked != latestBlocKnown) {
    console.log("[BANCOR] Checking blocks for logs");

    await populateSwapPools();

    console.info("[BANCOR] All pools are loaded");
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
    const immutable = true;

    const name = await poolContract.methods.name().call();
    const symbol = await poolContract.methods.symbol().call();
    const decimals = await poolContract.methods.decimals().call();

    let owner;
    if (pool_pairs_map.has(poolID)) {
      const p = pool_pairs_map.get(poolID);
      if (p.owner) {
        owner = p.owner;
      }
    } else {
      owner = await poolContract.methods.owner().call();
    }

    const poolTokensContract1 = await new web3.eth.Contract(
      poolTokensABI1,
      owner
    );
    const poolTokensContract2 = await new web3.eth.Contract(
      poolTokensABI2,
      owner
    );

    let reserveTokens = new Array();

    // Contracts follow strange patterns, not having some functions and other functions having to be called differently
    // So, if we go the catch statment we try the second method and if that also fails, we fail the pool
    let tokenStandart = true;
    try {
      reserveTokens = await poolTokensContract1.methods.reserveTokens().call();
    } catch (error) {
      tokenStandart = false;
      // Get the total of tokens in the pool
      const connectorTokenCount = await poolTokensContract2.methods
        .connectorTokenCount()
        .call();

      for (var i = 0; i < connectorTokenCount; i++) {
        // Pull the tokens in the pool
        const connectorToken = await poolTokensContract2.methods
          .connectorTokens(i)
          .call();
        reserveTokens.push(connectorToken.toLowerCase());
      }
    }

    swap_fee =
      ((await poolTokensContract1.methods.conversionFee().call()) / 1e6) * 100;

    for (var i in reserveTokens) {
      let token = await getTokenData(reserveTokens[i].toLowerCase());
      if (token === null) {
        fs.appendFileSync("tokens.txt", reserveTokens[i].toLowerCase() + "\n");
        console.error(
          "[BANCOR] Token not found: ",
          reserveTokens[i].toLowerCase()
        );
        continue;
      }

      if (tokenStandart) {
        const weight = await poolTokensContract1.methods
          .reserveWeight(reserveTokens[i].toLowerCase())
          .call();

        token.weight = ((weight / 1e6) * 100).toString();

        const reserveBalances = await poolTokensContract1.methods
          .reserveBalances()
          .call();

        token.reserves = Decimal(reserveBalances[i])
          .dividedBy("1e" + token.decimals)
          .toString();
        tokens.push(token);
      } else {
        const reserves = await poolTokensContract2.methods
          .reserves(reserveTokens[i])
          .call();

        token.weight = ((reserves.weight / 1e6) * 100).toString();

        token.reserves = Decimal(reserves.balance)
          .dividedBy("1e" + token.decimals)
          .toString();

        tokens.push(token);
      }
    }

    return {
      chain: "ETHEREUM",
      protocol: "BANCOR",
      pool_id: poolID,
      owner: owner,
      symbol: symbol,
      name: name,
      swap_fee: swap_fee.toString(),
      decimals: decimals.toString(),
      immutable: immutable,
      tokens: tokens,
    };
  } catch (error) {
    console.error(
      "[BANCOR] getPoolData",
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
      "[BANCOR] loadListFromMemory",
      error.name + ":" + error.message
    );
  }
};

const savePoolToMap = async (block) => {
  return new Promise((resolve) => {
    for (var i = 0; i < block.length; i++) {
      pools_loading.set(
        "0x" + block[i].topics[1].slice(26).toString().toLowerCase(),
        true
      );
    }
    if (block.length > 0) {
      console.info(
        "[BANCOR] Current number of pools to be loaded",
        pools_loading.size
      );
    }
    resolve();
  });
};

const createNewPool = async (poolID) => {
  if (bancor_not_valid.has(poolID)) {
    console.info(
      "[BANCOR] Problem creating a pair because pool is not valid",
      poolID + "\n"
    );
    return false;
  }

  const pair = await getPoolData(poolID);
  if (!pair) {
    bancor_not_valid.set(poolID, true);
    console.info("[BANCOR] Problem creating a pair for pool", poolID + "\n");
    return false;
  }

  if (!pool_pairs_map.has(poolID)) {
    totalPools++;
  }

  pool_pairs_map.set(poolID, pair);

  await redisClient.hset("POOLS:ETHEREUM:BANCOR", poolID, JSON.stringify(pair));

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
      "[BANCOR] All pools are update for a total of",
      totalPools,
      "pools in the latest block",
      latestBlocKnown
    );
    runAgain = false;
  }

  const block = await web3.eth.getPastLogs({
    fromBlock: fromBlock,
    toBlock: toBlock,
    address: BANCOR_NETWORK_ADDRESS,
    topics: [LOG_CONVERSION],
  });

  await savePoolToMap(block);

  fromBlock = toBlock;
  toBlock += 10000;

  if (runAgain) {
    await populateSwapPools();
  } else {
    // All done, lets save it to
    if (pools_loading.size > 0) {
      console.info("[BANCOR] Saving pools in memory: ", pools_loading.size);
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
    const poolID = "0x" + sync.topics[1].slice(26).toString().toLowerCase();
    lazy_load_map.set(poolID, true);

    if (!pool_pairs_map.has(poolID)) {
      if (bancor_not_valid.has(poolID)) {
        console.info("[BANCOR] streamWorker pool id not valid", poolID + "\n");
        return true;
      }
    }

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

    ZMQ.zmqSendMsg("TICKERS_ETHEREUM_BANCOR", poolID, JSON.stringify(ticker));

    await redisClient.hset(
      "POOLS:ETHEREUM:BANCOR",
      poolID,
      JSON.stringify(pair)
    );

    await redisClient.set(
      "ACTIVE:ETHEREUM:BANCOR:" + poolID,
      JSON.stringify(pair)
    );
    await redisClient.expire("ACTIVE:ETHEREUM:BANCOR:" + poolID, 86400);

    msgSequence++;

    return true;
  } catch (error) {
    console.error("[BANCOR] streamWorker", error.name + ":" + error.message);
  }
}

const swapWatcher = async () => {
  chainId = await web3.eth.getChainId();

  web3.eth
    .subscribe("logs", {
      address: BANCOR_NETWORK_ADDRESS,
      topics: [LOG_CONVERSION],
    })
    .on("connected", async () => {
      console.log(
        "[BANCOR] Connected to Ethereum: Listening for syncs on CHAIN ID:" +
          chainId
      );
    })
    .on("data", async (sync) => {
      streamQueue.push(sync);
    })
    .on("error", async (error) => {
      console.log("[BANCOR]:" + error);
    });
};

module.exports = {
  init,
};
