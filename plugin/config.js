const configurator = require('json-evn-configurator');

const cassandraTables = require('../cassandraSchemas/cassandra-tables');

const cassandraConf = configurator(require('./cassandra-config'), 'CASSANDRA');

const commandUpdatesTables = cassandraTables.commandUpdatesTables;
cassandraConf.CUSTOM.COMMAND_UPDATES_STORING = commandUpdatesTables && !!commandUpdatesTables.length;

module.exports = {
    cassandra: cassandraConf
};