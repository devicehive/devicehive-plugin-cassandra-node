class Metadata {
    constructor(md) {
        this._md = md;
    }

    /**
     * Returns true if JSON schema is the same as metadata of real schema
     * @param {JSONSchema} jsonSchema
     * @returns {boolean}
     */
    isSameColumnsSchema(jsonSchema) {
        const schemaColumns = Object.keys(jsonSchema.getColumns()).map(col => col.toLowerCase());
        const realTableColumns = Object.keys(this._md.columnsByName);

        if (schemaColumns.length !== realTableColumns.length) {
            return false;
        }

        return schemaColumns.every(col => realTableColumns.includes(col));
    }

    /**
     * Returns true if column exists in this metadata
     * @param {string} colName
     * @returns {boolean}
     */
    columnExists(colName) {
        return colName in this._md.columnsByName;
    }

    /**
     * Returns full name of type with its types if type is complex
     * @param {string} colName Column name
     * @returns {string}
     */
    getColumnFullTypeName(colName) {
        const type = this._md.columnsByName[colName].type;
        let name = Metadata.getDataTypeNameByCode(type.code);

        if (type.info && type.info.length) {
            const nestedTypes = [];
            type.info.forEach(t => {
                nestedTypes.push(Metadata.getDataTypeNameByCode(t.code));
            });

            name += `<${nestedTypes.join(',')}>`;
        } else if (type.info && type.info.name) {
            name = type.info.name;
        }

        return name;
    }

    /**
     * Returns data type name by given numeric code
     * @param {Number} code
     * @returns {string|undefined}
     */
    static getDataTypeNameByCode(code) {
        return Metadata._dataTypesByCode[code];
    }
}

Metadata._dataTypesByCode = {
    1: 'ascii',
    2: 'bigint',
    3: 'blob',
    4: 'boolean',
    5: 'counter',
    6: 'decimal',
    7: 'double',
    8: 'float',
    9: 'int',
    10: 'text',
    11: 'timestamp',
    12: 'uuid',
    13: 'varchar',
    14: 'varint',
    15: 'timeuuid',
    16: 'inet',
    17: 'date',
    18: 'time',
    19: 'smallint',
    20: 'tinyint',
    32: 'list',
    33: 'map',
    34: 'set',
    48: 'udt',
    49: 'tuple'
};

module.exports = Metadata;