const JSONSchema = require('./JSONSchema');
const BaseSchemaBuilder = require('./BaseSchemaBuilder');

class UDTSchemaBuilder extends BaseSchemaBuilder {
    constructor() {
        super();
        this._structureType = UDTSchemaBuilder.TYPE_STRUCTURE;
    }

    /**
     * Specifies create type of query
     * @param [typeName = '']
     * @returns {UDTSchemaBuilder}
     */
    createType(typeName = '') {
        return this._create(typeName);
    }

    /**
     * Specifies drop type of query
     * @param [typeName = '']
     * @returns {UDTSchemaBuilder}
     */
    dropType(typeName) {
        return this._drop(typeName);
    }

    /**
     * Specifies type schema in JSON format
     * @param {object} schemaDescription
     * @returns {UDTSchemaBuilder}
     */
    fromJSONSchema(schemaDescription) {
        const jsonSchema = new JSONSchema(schemaDescription);

        this._definition = `(${jsonSchema.buildColumnsDefinition()})`;

        return this;
    }
}

module.exports = UDTSchemaBuilder;