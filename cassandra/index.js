const cassandraDriver = require('cassandra-driver');
const debug = require('debug')('cassandrastoragemodule');

const CassandraStorage = require('./src/CassandraStorage');
const CassandraStorageLogger = require('./src/CassandraStorageLogger');
const Utils = require('./lib/Utils');
const CassandraConfigurator = require('./src/CassandraConfigurator');

const defaultConfig = require('./config');

module.exports = {
    /**
     * Creates client connection to Cassandra
     * @returns {Promise<CassandraStorage>}
     */
    connect(userConfig = {}) {
        const config = createConfig(userConfig);
        const cassandraClient = new cassandraDriver.Client(config.connection);
        const logger = new CassandraStorageLogger(debug);
        const cassandraStorageProvider = new CassandraStorage(cassandraClient);

        logger.attach(cassandraStorageProvider);

        return cassandraClient.connect().then(() => cassandraStorageProvider);
    }
};

function createConfig(userConfig) {
    const conf = Utils.merge({}, defaultConfig, CassandraConfigurator.normalizeConfig(userConfig));
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
