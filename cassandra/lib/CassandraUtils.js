const Utils = require('./Utils');

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
     * Cast to string if Cassandra column is text, varchar or ascii type
     * @param {string} type
     * @param {any} val
     * @returns {string | any}
     */
    static cassandraStringTypeOrDefault(type, val = null) {
        const stringTypes = [ 'text', 'varchar', 'ascii' ];

        if (stringTypes.includes(type) && val !== null) {
            return Utils.isObject(val) ? JSON.stringify(val) : val.toString();
        }

        return val;
    }

    static replaceTypeAliases(str) {
        return str.replace(/varchar/g, 'text');
    }
}

module.exports = CassandraUtils;