const configurator = require('json-evn-configurator');

const Utils = require('./Utils');
const cassandraTables = require('../cassandraSchemas/cassandra-tables');

const IGNORE_CONFIG_CASE = true;
const cassandraConf = configurator(require('./cassandra-config'), 'cassandra', IGNORE_CONFIG_CASE);

cassandraConf.custom.commandUpdatesStoring = Utils.isNotEmpty(cassandraTables.commandUpdatesTables);

module.exports = {
    cassandra: cassandraConf
};