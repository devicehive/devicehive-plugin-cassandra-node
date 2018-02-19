const cassandraConfig = require('../config').cassandra;
const cassandraTables = require('../../cassandraSchemas/cassandra-tables').tables;
const cassandraUDTs = require('../../cassandraSchemas/cassandra-user-types');
const SchemaValidator = require('./SchemaValidator');
const createSchema = require('./createSchema');

exitIfInvalid(cassandraTables);

createSchema(cassandraConfig, cassandraUDTs, cassandraTables).then(() => {
    console.log('Cassandra schemas have been created');
    process.exit(0);
}).catch(err => {
    console.error(`CASSANDRA SCHEMAS HAVEN'T BEEN CREATED`);
    console.error(err);
    process.exit(1);
});

function exitIfInvalid(schemas) {
    const errors = SchemaValidator.getSchemasErrors(schemas);

    errors.forEach(err => console.error(err.toString()));

    if (errors.length) {
        process.exit(1);
    }
}