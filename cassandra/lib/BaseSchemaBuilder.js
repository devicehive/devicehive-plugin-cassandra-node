const Utils = require('./Utils');

class BaseSchemaBuilder {
    constructor() {
        this._queryString = '';
        this._queryType = null;
        this._name = '';
        this._definition = '';
        this._ifConditionExists = false;
        this._structureType = '';
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
        return `${this._queryString} ${this._name}${this._definition}`;
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
        if (this._queryType === BaseSchemaBuilder.CREATE_QUERY_TYPE && !this._ifConditionExists) {
            this._ifConditionExists = true;
            this._queryString += ' IF NOT EXISTS';
        }

        return this;
    }

    /**
     * Drop structure only if it exists
     * @returns {BaseSchemaBuilder}
     */
    ifExists() {
        if (this._queryType === BaseSchemaBuilder.DROP_QUERY_TYPE && !this._ifConditionExists) {
            this._ifConditionExists = true;
            this._queryString += ' IF EXISTS';
        }

        return this;
    }

    /**
     * Specifies create type of query
     * @param [name = '']
     * @returns {BaseSchemaBuilder}
     * @private
     */
    _create(name = '') {
        return this._query(BaseSchemaBuilder.CREATE_QUERY_TYPE, name);
    }

    /**
     * Specifies drop type of query
     * @param [name = '']
     * @returns {BaseSchemaBuilder}
     * @private
     */
    _drop(name = '') {
        return this._query(BaseSchemaBuilder.DROP_QUERY_TYPE, name);
    }

    /**
     * Specify query with given type
     * @param type
     * @param name
     * @returns {BaseSchemaBuilder}
     * @private
     */
    _query(type, name = '') {
        const queryTypes = {
            [BaseSchemaBuilder.CREATE_QUERY_TYPE]: 'CREATE',
            [BaseSchemaBuilder.DROP_QUERY_TYPE]: 'DROP'
        };

        this._queryType = type;
        this._queryString = `${queryTypes[type]} ${this._structureType}`;

        return Utils.isEmpty(name) ? this : this.withName(name);
    }

    static get CREATE_QUERY_TYPE() { return 0; }
    static get DROP_QUERY_TYPE() { return 1; }

    static get TYPE_STRUCTURE() { return 'TYPE'; }
    static get TABLE_STRUCTURE() { return 'TABLE'; }
}

module.exports = BaseSchemaBuilder;