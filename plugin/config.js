const configurator = require('json-evn-configurator');

const cassandraConf = require('./cassandra-config');

module.exports = {
    cassandra: configurator(cassandraConf, 'CASSANDRA')
};