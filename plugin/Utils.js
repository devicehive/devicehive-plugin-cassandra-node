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
}

module.exports = Utils;