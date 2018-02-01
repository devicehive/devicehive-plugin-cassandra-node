const cassandraDriver = require('cassandra-driver');

const CassandraStorage = require('./src/CassandraStorage');
const Utils = require('./lib/Utils');
const CassandraConfigurator = require('./src/CassandraConfigurator');

const defaultConfig = require('./config');

module.exports = {
    /**
     * Creates client connection to Cassandra
     * @returns {CassandraStorage}
     */
    connect(userConfig = {}) {
        const config = createConfig(userConfig);
        const cassandraClient = new cassandraDriver.Client(config.connection);

        return new CassandraStorage(cassandraClient);
    }
};

function createConfig(userConfig) {
    const conf = CassandraConfigurator.normalizeConfig(Utils.merge({}, defaultConfig, userConfig));
    const configurator = new CassandraConfigurator(conf.connection);

    configurator.configAuthProvider()
        .configReconnection()
        .configAddressResolution()
        .configSpeculativeExecution()
        .configTimestampGeneration()
        .configLoadBalancing()
        .configRetry();


    conf.connection = configurator.config;

    return conf;
}
