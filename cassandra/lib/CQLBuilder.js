const QueryBuilder = require('./QueryBuilder');
const TableSchemaBuilder = require('./TableSchemaBuilder');
const UDTSchemaBuilder = require('./UDTSchemaBuilder');

class CQLBuilder {
    static get INSERT_QUERY() { return 'insert'; }
    static get UPDATE_QUERY() { return 'update'; }

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
     * Creates update type of query
     * @param tableName
     * @param [keyspace = '']
     * @returns {QueryBuilder}
     */
    static update(tableName, keyspace) {
        return new QueryBuilder().update(tableName, keyspace);
    }

    /**
     * Creates query based on given type
     * @param type CQLBuilder.INSERT_QUERY or CQLBuilder.UPDATE_QUERY
     * @returns {QueryBuilder}
     */
    static query(type) {
        const queryBuilder = new QueryBuilder();

        if (type === CQLBuilder.INSERT_QUERY) {
            queryBuilder.insertInto();
        } else if (type === CQLBuilder.UPDATE_QUERY) {
            queryBuilder.update();
        }

        return queryBuilder;
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

    static dropTable(tableName = '') {
        return new TableSchemaBuilder().dropTable(tableName);
    }

    static dropType(typeName = '') {
        return new UDTSchemaBuilder().dropType(typeName);
    }
}


module.exports = CQLBuilder;