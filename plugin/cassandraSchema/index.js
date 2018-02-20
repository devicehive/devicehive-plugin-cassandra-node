const cassandraConfig = require('../config').cassandra;
const cassandraTables = require('../../cassandraSchemas/cassandra-tables');
const cassandraUDTs = require('../../cassandraSchemas/cassandra-user-types');
const CassandraStorage = require('../../cassandra/index');
const SchemaCreator = require('./SchemaCreator');
const SchemaValidator = require('./SchemaValidator');

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
    const schemas = {
        udt: cassandraUDTs,
        tables: cassandraTables.tables
    };
    let schemaCreator;

    return CassandraStorage.connect(cassandraConfig).then(cassandra => {
        schemaCreator = new SchemaCreator(cassandra);
        return schemaCreator.dropBeforeCreate(schemas);
    }).then(() => schemaCreator.create(schemas));
}

function exitIfInvalid(schemas) {
    const errors = SchemaValidator.getSchemasErrors(schemas);

    errors.forEach(err => console.error(err.toString()));

    if (errors.length) {
        process.exit(1);
    }
}