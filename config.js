const systemconfig = {
  redis: {
    HOST: "redis.zinnion.com",
    AUTH: "Br@sa154",
  },
  zmq: {
    HOST: "cheetah.zinnion.com",
    PORT: 31337,
  },
  web3_eth: {
    HOST: "web3.zinnion.com",
    PORT: 8585,
    PROTOCOL: "ws",
  },
  web3_bsc: {
    HOST: "bsc.zinnion.com",
    //PORT: 8547, // Mainnet
    PORT: 7547, // Testnet
    PROTOCOL: "ws",
  },
};

module.exports = {
  systemconfig,
};
