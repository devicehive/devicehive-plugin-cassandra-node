const Utils = require('./Utils');

class BaseSchemaBuilder {
    static get CREATE_QUERY_TYPE() { return 0; }

    constructor() {
        this._query = '';
        this._queryType = null;
        this._name = '';
        this._definition = '';
        this._ifNotExists = '';
    }

    /**
     * Specifies schema in JSON format
     * @param {object} schemaDescription
     * @returns {BaseSchemaBuilder}
     */
    fromJSONSchema(schemaDescription) {
        throw new TypeError('Abstract BaseSchemaBuilder.fromJSONSchema() is not implemented');
    }

    /**
     * Builds query string
     * @returns {string}
     */
    build() {
        return `${this._query} ${this._ifNotExists}${this._name}${this._definition}`;
    }

    /**
     * Specifies name of structure to be created
     * @param {string} name
     * @returns {BaseSchemaBuilder}
     */
    withName(name) {
        this._name = name;
        return this;
    }

    /**
     * Create structure only if it not exists
     * @returns {BaseSchemaBuilder}
     */
    ifNotExists() {
        if(this._queryType === BaseSchemaBuilder.CREATE_QUERY_TYPE) {
            // @TODO Remove this variable, use append to this._query and appropriate flag for IF NOT EXISTS
            this._ifNotExists = 'IF NOT EXISTS ';
        }

        return this;
    }

    /**
     * Specifies type of query
     * @param {string} structureType
     * @param [name = '']
     * @returns {BaseSchemaBuilder}
     */
    _create(structureType, name = '') {
        this._queryType = BaseSchemaBuilder.CREATE_QUERY_TYPE;        
        this._query = `CREATE ${structureType}`;
        
        return Utils.isEmpty(name) ? this : this.withName(name);
    }
}

module.exports = BaseSchemaBuilder;