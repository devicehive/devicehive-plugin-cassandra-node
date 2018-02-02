const cassandraConfig = require('./config').cassandra;
const cassandraTables = require('../cassandraSchemas/cassandra-tables');
const cassandraUDTs = require('../cassandraSchemas/cassandra-user-types');
const CassandraStorage = require('../cassandra/index');

const cassandra = CassandraStorage.connect(cassandraConfig);

cassandra.createUDTSchemas(cassandraUDTs)
    .then(() => cassandra.createTableSchemas(cassandraTables.tables))
    .then(() => {
        console.log('Cassandra schemas have been created');
        process.exit(0);
    })
    .catch(err => {
        console.error(`CASSANDRA SCHEMAS HAVEN'T BEEN CREATED`);
        console.error(err);
        process.exit(1);
    });