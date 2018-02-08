const configurator = require('json-evn-configurator');

const Utils = require('./Utils');
const cassandraTables = require('../cassandraSchemas/cassandra-tables');

const cassandraConf = configurator(require('./cassandra-config'), 'CASSANDRA');

cassandraConf.CUSTOM.COMMAND_UPDATES_STORING = Utils.isNotEmpty(cassandraTables.commandUpdatesTables);

module.exports = {
    cassandra: cassandraConf
};