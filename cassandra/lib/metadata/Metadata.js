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
     * @param {string} propName
     * @returns {boolean}
     */
    columnExists(propName) {
        throw new TypeError('columnExists() is not implemented');
    }

    _getTypeDescription(propName) {
        throw new TypeError('_getTypeDescription() is not implemented');
    }

    /**
     * Returns true if schema has same primary key as metadata
     * @param jsonSchema
     * @returns {boolean}
     */
    isSamePrimaryKey(jsonSchema) {
        return true;
    }

    /**
     * Returns true if schema has same clustering key as metadata
     * @param jsonSchema
     * @returns {boolean}
     */
    isSameClusteringKey(jsonSchema) {
        return true;
    }

    /**
     * Returns true if schema has same ordering as metadata
     * @param jsonSchema
     * @returns {boolean}
     */
    isSameOrdering(jsonSchema) {
        return true;
    }

    /**
     * Returns true if JSON schema is the same as metadata of real schema (contains same members)
     * @param {JSONSchema} jsonSchema
     * @param {Array<string>} metadataProps Array of member names of structure (table or UDT)
     * @returns {boolean}
     * @private
     */
    _sameStructure(jsonSchema, metadataProps) {
        const schemaProps = Object.keys(jsonSchema.getProperties()).map(prop => prop.toLowerCase());

        if (schemaProps.length !== metadataProps.length) {
            return false;
        }

        return schemaProps.every(prop => metadataProps.includes(prop));
    }

    /**
     * Returns full name of type with its types if type is complex
     * @param {string} propName Field or column name
     * @returns {string}
     */
    getFullTypeName(propName) {
        propName = propName.toLowerCase();
        
        const type = this._getTypeDescription(propName);
        let name = this._complexType(propName) || this._customType(propName) || this._simpleType(propName);

        if (type.options && type.options.frozen) {
            name = `frozen<${name}>`;
        }

        return name;
    }

    /**
     * Returns full definition of property type if it's complex type
     * @param {string} propName Column or field name
     * @returns {string}
     * @private
     */
    _complexType(propName) {
        const type = this._getTypeDescription(propName);

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

    /**
     * Returns custom type name if property is UDT
     * @param {string} propName Column or field name
     * @returns {string}
     * @private
     */
    _customType(propName) {
        const type = this._getTypeDescription(propName);

        if (type.info && type.info.name) {
            return type.info.name;
        }

        return '';
    }

    /**
     * Returns just type name if property field is simple type
     * @param {string} propName Column or field name
     * @returns {string}
     * @private
     */
    _simpleType(propName) {
        const type = this._getTypeDescription(propName);
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