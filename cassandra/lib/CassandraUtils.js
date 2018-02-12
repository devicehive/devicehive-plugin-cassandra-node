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
}

module.exports = CassandraUtils;