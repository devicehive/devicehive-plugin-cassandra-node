const { Insert } = require('cassandra-cql-builder');
const JSONSchema = require('./JSONSchema');

class QueryBuilder {
    constructor() {
        this._query = null;
        this._schema = null;
        this._queryParams = null;
    }

    /**
     * Specifies which table data will be inserted into
     * @param tableName
     * @param [keyspace = '']
     * @returns {QueryBuilder}
     */
    insertInto(tableName, keyspace = '') {
        this._query = Insert().table(tableName, keyspace);
        return this;
    }

    /**
     * Specifies query parameters
     * @param data
     * @returns {QueryBuilder}
     */
    queryParams(data) {
        this._queryParams = data;
        return this;
    }

    /**
     * Sets JSON schema of data to filter query parameters
     * @param schema
     * @returns {QueryBuilder}
     */
    withJSONSchema(schema) {
        this._schema = new JSONSchema(schema);
        return this;
    }

    /**
     * Applies custom types to schema
     * @param types
     * @returns {QueryBuilder}
     */
    withJSONCustomTypes(types = null) {
        if (this._schema) {
            this._schema.fillWithTypes(types);
        }

        return this;
    }

    /**
     * Builds query
     * @returns {Object} cql
     * @returns {string} cql.query
     * @returns {Array} cql.params
     */
    build() {
        this._fillQueryWithValues();
        return this._query.build();
    }

    /**
     * Sets query parameters as values in query
     * @private
     */
    _fillQueryWithValues() {
        if (this._query.command.name !== 'INSERT') {
            return;
        }

        let queryParams = this._queryParams;
        if (this._schema) {
            queryParams = this._schema.filterData(queryParams);
        }

        for (let key in queryParams) {
            if (queryParams.hasOwnProperty(key)) {
                this._query.value(key, queryParams[key]);
            }
        }

        this._queryParams = {};
    }
}

module.exports = QueryBuilder;