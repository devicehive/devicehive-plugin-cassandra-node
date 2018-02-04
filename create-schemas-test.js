const cassandraConfig = require('./plugin/config').cassandra;

cassandraConfig.CONNECTION.KEYSPACE = 'testks';

require('./plugin/createCassandraSchemas');