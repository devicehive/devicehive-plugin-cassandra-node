const config = require('../cassandraConfig/config');

config.CONNECTION.POLICIES = require('./../cassandraConfig/policies');

module.exports = config;