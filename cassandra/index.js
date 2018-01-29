const cassandraDriver = require('cassandra-driver');

const CassandraStorage = require('./src/CassandraStorage');
const Utils = require('./lib/Utils');
const CassandraUtils = require('./lib/CassandraUtils');

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
    const conf = CassandraUtils.normalizeConfig(Utils.merge({}, defaultConfig, userConfig));
    const { username, password } = conf.connection;

    if(username && password) {
        conf.connection.authProvider = new cassandraDriver.auth.PlainTextAuthProvider(username, password);
    }

    return conf;
}
