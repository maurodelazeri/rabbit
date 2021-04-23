// require("log-timestamp")(function () {
//   return new Date().toISOString() + " %s";
// });
const Web3 = require("web3");
//const connection = require("./connection");
const config = require("./config");

const { init: InitUniSwapV2 } = require("./protocols/uniswapv2/");
const { init: InitSushiSwap } = require("./protocols/sushiswap/");
const { init: InitBalancer } = require("./protocols/balancer/");
const { init: InitBancor } = require("./protocols/bancor/");
const { init: InitCurveFi } = require("./protocols/curvefi/");
const { init: InitPancakeswap } = require("./protocols/pancakeswap/");
const { init: InitBurgerswap } = require("./protocols/burgerswap/");

try {
  var options = {
    timeout: 30000, // ms
    // Useful if requests result are large
    clientConfig: {
      maxReceivedFrameSize: 100000000, // bytes - default: 1MiB
      maxReceivedMessageSize: 100000000, // bytes - default: 8MiB
    },
    // Enable auto reconnection
    reconnect: {
      auto: true,
      delay: 5000, // ms
      maxAttempts: 15,
      onTimeout: false,
    },
  };

  if (process.env.CHAIN === "ethereum") {
    web3 = new Web3(
      new Web3.providers.WebsocketProvider(
        `${config.systemconfig.web3_eth.PROTOCOL}://${config.systemconfig.web3_eth.HOST}:${config.systemconfig.web3_eth.PORT}`,
        options
      )
    );
    console.log(
      `[BAGDER] Connected successfully to ${config.systemconfig.web3_eth.PROTOCOL}://${config.systemconfig.web3_eth.HOST}:${config.systemconfig.web3_eth.PORT}!`
    );
  } else if (process.env.CHAIN === "bsc") {
    web3 = new Web3(
      new Web3.providers.WebsocketProvider(
        `${config.systemconfig.web3_bsc.PROTOCOL}://${config.systemconfig.web3_bsc.HOST}:${config.systemconfig.web3_bsc.PORT}`,
        options
      )
    );
    console.log(
      `[BAGDER] Connected successfully to ${config.systemconfig.web3_bsc.PROTOCOL}://${config.systemconfig.web3_bsc.HOST}:${config.systemconfig.web3_bsc.PORT}!`
    );
  } else {
    console.log("Please set the env CHAIN");
    process.exit(1);
  }

  //const checkConnectionPromise = connection.checkConnection(ip, port);
  const args = process.argv;

  switch (args[2]) {
    case "uniswapv2":
      InitUniSwapV2(web3);
      break;
    case "sushiswap":
      InitSushiSwap(web3);
      break;
    case "balancer":
      InitBalancer(web3);
      break;
    case "bancor":
      InitBancor(web3);
      break;
    case "curvefi":
      InitCurveFi(web3);
      break;
    case "pancakeswap":
      InitPancakeswap(web3);
      break;
    case "burgerswap":
      InitBurgerswap(web3);
      break;
    default:
      console.log("[PLEASE SELECT THE TARGEG: ex: uniswap]");
      process.exit();
  }
} catch (error) {
  console.error("[BAGDER] ", error.name + ":" + error.message);
}
