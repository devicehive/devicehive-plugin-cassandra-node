const config = require('../cassandraConfig/config');

config.connection.policies = require('../cassandraConfig/policies');

module.exports = config;