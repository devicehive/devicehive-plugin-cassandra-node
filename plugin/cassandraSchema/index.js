const cassandraConfig = require('../config').cassandra;
const cassandraTables = require('../../cassandraSchemas/cassandra-tables');
const cassandraUDTs = require('../../cassandraSchemas/cassandra-user-types');
const CassandraStorage = require('../../cassandra/index');
const SchemaCreator = require('./SchemaCreator');

exitIfInvalid(cassandraTables.tables);

createSchema().then(() => {
    console.log('Cassandra schemas have been created');
    process.exit(0);
}).catch(err => {
    console.error(`CASSANDRA SCHEMAS HAVEN'T BEEN CREATED`);
    console.error(err);
    process.exit(1);
});

function createSchema() {
    return CassandraStorage.connect(cassandraConfig).then(cassandra => {
        return new SchemaCreator(cassandra).create({
            udt: cassandraUDTs,
            tables: cassandraTables.tables
        });
    });
}

function exitIfInvalid(schemas) {
    const errors = SchemaCreator.getSchemasErrors(schemas);

    errors.forEach(err => console.error(err.toString()));

    if (errors.length) {
        process.exit(1);
    }
}