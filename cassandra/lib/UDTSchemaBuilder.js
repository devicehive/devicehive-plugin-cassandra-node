const JSONSchema = require('./JSONSchema');
const BaseSchemaBuilder = require('./BaseSchemaBuilder');

class UDTSchemaBuilder extends BaseSchemaBuilder {
    /**
     * Specifies type of query
     * @param [typeName = '']
     * @returns {UDTSchemaBuilder}
     */
    createType(typeName = '') {
        return this._create('TYPE', typeName);
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