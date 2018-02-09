class CassandraUtils {
    /**
     * Extracts pure type name from column type definition
     * @param {string} str
     * @returns {string}
     */
    static extractTypeName(str) {
        /* @TODO Extraction of UDT name from nested structures e.g. frozen<list<tuple<UDT_NAME, int>>> */
        return str ? str.replace(/frozen<|>/ig, '') : '';
    }

    /**
     * Returns full name of type with its types if type is complex
     * @param {object} typeDescriptor cassandra-driver metadata type object
     * @returns {string}
     */
    static fullTypeName(typeDescriptor) {
        const descr = typeDescriptor;
        let name = CassandraUtils.getDataTypeNameByCode(descr.code);

        if (descr.info) {
            const nestedTypes = [];
            descr.info.forEach(type => {
                nestedTypes.push(CassandraUtils.getDataTypeNameByCode(type.code));
            });

            name += `<${nestedTypes.join(',')}>`;
        }

        return name;
    }

    /**
     * Returns data type name by given numeric code
     * @param {Number} code
     * @returns {string|undefined}
     */
    static getDataTypeNameByCode(code) {
        return CassandraUtils._dataTypesByCode[code];
    }
}

CassandraUtils._dataTypesByCode = {
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

module.exports = CassandraUtils;