const QueryBuilder = require('./QueryBuilder');
const TableSchemaBuilder = require('./TableSchemaBuilder');
const UDTSchemaBuilder = require('./UDTSchemaBuilder');

class CQLBuilder {
    /**
     * Creates insert type of query
     * @param tableName
     * @param [keyspace = '']
     * @returns {QueryBuilder}
     */
    static insertInto(tableName, keyspace = '') {
        return new QueryBuilder().insertInto(tableName, keyspace);
    }

    /**
     * Creates table creation type of query
     * @param tableName
     * @returns {TableSchemaBuilder}
     */
    static createTable(tableName = '') {
        return new TableSchemaBuilder().createTable(tableName);
    }

    /**
     * Creates UDT creation type of query
     * @param typeName
     * @returns {UDTSchemaBuilder}
     */
    static createUDT(typeName = '') {
        return new UDTSchemaBuilder().createType(typeName);
    }
}


module.exports = CQLBuilder;