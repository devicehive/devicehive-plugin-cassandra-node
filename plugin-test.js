const cassandraConfig = require('./plugin/config').cassandra;

cassandraConfig.CONNECTION.KEYSPACE = 'testks';

const CassandraPlugin = require('./plugin/CassandraPluginService');

new CassandraPlugin().initCassandra().then(cass => {
    console.log('Cassandra initialized');
}).catch(console.error);