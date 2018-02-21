const CassandraStorage = require('../../cassandra/index');
const SchemaCreator = require('./SchemaCreator');

function createSchema(cassandraConfig, cassandraUDTs, cassandraTables) {
    const schemas = {
        udt: cassandraUDTs,
        tables: cassandraTables
    };
    let schemaCreator;

    return CassandraStorage.connect(cassandraConfig).then(cassandra => {
        schemaCreator = new SchemaCreator(cassandra);
        return schemaCreator.dropBeforeCreate(schemas);
    }).then(() => schemaCreator.create(schemas));
}

module.exports = createSchema;