const JSONSchema = require('./JSONSchema');
const BaseSchemaBuilder = require('./BaseSchemaBuilder');

class TableSchemaBuilder extends BaseSchemaBuilder {
    constructor() {
        super();
        this._structureType = TableSchemaBuilder.TABLE_STRUCTURE;
    }

    /**
     * Specifies create type of query
     * @param [tableName = '']
     * @returns {TableSchemaBuilder}
     */
    createTable(tableName = '') {
        return this._create(tableName);
    }

    /**
     * Specifies drop type of query
     * @param [tableName = '']
     * @returns {TableSchemaBuilder}
     */
    dropTable(tableName) {
        return this._drop(tableName);
    }

    /**
     * Specifies table schema in JSON format
     * @param {object} schemaDescription
     * @returns {TableSchemaBuilder}
     */
    fromJSONSchema(schemaDescription) {
        const jsonSchema = new JSONSchema(schemaDescription);

        const columns = jsonSchema.buildColumnsDefinition();
        const keys = jsonSchema.buildKeys();
        const tableConfig = jsonSchema.buildTableConfiguration();

        this._definition = `(${columns},${keys}) ${tableConfig}`.trim();

        return this;
    }
}

module.exports = TableSchemaBuilder;