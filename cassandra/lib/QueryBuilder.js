const { Insert, Update } = require('cassandra-cql-builder');
const JSONSchema = require('./JSONSchema');

class QueryBuilder {
    constructor() {
        this._query = null;
        this._schema = null;
        this._queryParams = null;
    }

    /**
     * Specifies insert type of query and which table data will be inserted into
     * @param tableName
     * @param [keyspace = '']
     * @returns {QueryBuilder}
     */
    insertInto(tableName, keyspace = '') {
        this._query = Insert().table(tableName, keyspace);
        return this;
    }

    /**
     * Specifies update type of query and table to update
     * @param tableName
     * @param keyspace
     * @returns {QueryBuilder}
     */
    update(tableName, keyspace = '') {
        this._query = Update().table(tableName, keyspace);
        return this;
    }

    /**
     * Sets query condition
     * @param key Must be specified with operator, i.e. key >= ?
     * @param value
     * @returns {QueryBuilder}
     */
    where(key, value) {
        if (this._query.where) {
            this._query.where(key, value);
        }

        return this;
    }

    /**
     * Specifies query parameters
     * @param data
     * @returns {QueryBuilder}
     */
    queryParams(data) {
        this._queryParams = { ...data };
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
        this._constructWhereCondition();
        this._fillQueryWithValues();
        return this._query.build();
    }

    /**
     * Sets WHERE condition for query, assigns only not key values to _queryParams after that
     * @private
     */
    _constructWhereCondition() {
        if (this._query.command.name !== 'UPDATE' || !this._schema || !this._queryParams) {
            return;
        }

        const keyValues = this._schema.extractKeys(this._queryParams);

        for (let k in keyValues) {
            this.where(`${k} = ?`, keyValues[k]);
        }

        this._queryParams = this._schema.extractNotKeys(this._queryParams);
    }

    /**
     * Sets query parameters as values in query
     * @private
     */
    _fillQueryWithValues() {
        if (this._query.command.name !== 'INSERT' && this._query.command.name !== 'UPDATE') {
            return;
        }

        let queryParams = this._queryParams;
        if (this._schema) {
            queryParams = this._schema.filterData(queryParams);
        }

        for (let key in queryParams) {
            if (queryParams.hasOwnProperty(key)) {
                this._value(key, queryParams[key]);
            }
        }

        this._queryParams = {};
    }

    _value(key, val) {
        if (this._query.command.name === 'INSERT') {
            this._query.value(key, val);
        } else if (this._query.command.name === 'UPDATE') {
            this._query.set(key, val);
        }

        return this;
    }
}

module.exports = QueryBuilder;