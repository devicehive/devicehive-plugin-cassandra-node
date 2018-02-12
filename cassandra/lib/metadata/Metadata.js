class Metadata {
    constructor(md) {
        this._md = md;
    }

    static create(md) {
        // Because of cyclic dependency we will require child classes right before instance creation
        const TableMetadata = require('./TableMetadata');
        const UDTMetadata = require('./UDTMetadata');

        if (md.columnsByName) {
            return new TableMetadata(md);
        } else if (md.fields) {
            return new UDTMetadata(md);
        }

        return null;
    }

    /**
     * Returns true if JSON schema is the same as metadata of real schema (contains same members)
     * @param {JSONSchema} jsonSchema
     * @returns {boolean}
     */
    isSameMembersSchema(jsonSchema) {
        throw new TypeError('isSameMembersSchema() is not implemented');
    }

    /**
     * Returns true if column exists in this metadata
     * @param {string} colName
     * @returns {boolean}
     */
    columnExists(colName) {
        throw new TypeError('columnExists() is not implemented');
    }

    _getTypeDescription(colName) {
        throw new TypeError('_getTypeDescription() is not implemented');
    }

    /**
     * Returns true if JSON schema is the same as metadata of real schema (contains same members)
     * @param {JSONSchema} jsonSchema
     * @param {Array<string>} structureMembers Array of member names of structure (table or UDT)
     * @returns {boolean}
     * @private
     */
    _sameStructure(jsonSchema, structureMembers) {
        const schemaColumns = Object.keys(jsonSchema.getColumns()).map(col => col.toLowerCase());

        if (schemaColumns.length !== structureMembers.length) {
            return false;
        }

        return schemaColumns.every(col => structureMembers.includes(col));
    }

    /**
     * Returns full name of type with its types if type is complex
     * @param {string} colName Column name
     * @returns {string}
     */
    getColumnFullTypeName(colName) {
        const type = this._getTypeDescription(colName);
        let name = this._complexType(colName) || this._customType(colName) || this._simpleType(colName);

        if (type.options && type.options.frozen) {
            name = `frozen<${name}>`;
        }

        return name;
    }

    _complexType(colName) {
        const type = this._getTypeDescription(colName);

        if (type.info && type.info.length) {
            const name = Metadata.getDataTypeNameByCode(type.code);

            const nestedTypes = [];
            type.info.forEach(t => {
                nestedTypes.push(Metadata.getDataTypeNameByCode(t.code));
            });


            return name + `<${nestedTypes.join(',')}>`;
        }

        return '';
    }

    _customType(colName) {
        const type = this._getTypeDescription(colName);

        if (type.info && type.info.name) {
            return type.info.name;
        }

        return '';
    }

    _simpleType(colName) {
        const type = this._getTypeDescription(colName);
        return Metadata.getDataTypeNameByCode(type.code);
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