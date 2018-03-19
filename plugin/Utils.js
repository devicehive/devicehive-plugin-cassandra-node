class Utils {
    /**
     * Returns true if argument is not empty string, array, null, undefined or 0
     * @param val
     * @returns {boolean}
     */
    static isNotEmpty(val) {
        return !Utils.isEmpty(val);
    }

    /**
     * Returns true if argument is empty string, array, null, undefined or 0
     * @param val
     * @returns {boolean}
     */
    static isEmpty(val) {
        return !val || !val.length;
    }

    /**
     * Returns true if argument is NOT object
     * @param val
     * @returns {boolean}
     */
    static isNotObject(val) {
        return !Utils.isObject(val);
    }

    /**
     * Returns true if argument is object
     * @param val
     * @returns {boolean}
     */
    static isObject(val) {
        const type = typeof val;
        return val !== null && (type === 'object');
    }
}

module.exports = Utils;