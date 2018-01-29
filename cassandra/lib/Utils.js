const merge = require('merge');

class Utils {
    /**
     * Returns true or false if argument is string 'true' or 'false' accordingly
     * @param str
     * @returns {boolean|any}
     */
    static booleanOrDefault(str) {
        return ({ 'true': true, 'false': false })[str] || str;
    }

    /**
     * Returns true if argument is object or function
     * @param val
     * @returns {boolean}
     */
    static isObject(val) {
        const type = typeof val;
        return val !== null && (type === 'object' || type === 'function');
    }

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

    static merge(...objects) {
        return merge.recursive(...objects);
    }

    static copy(object) {
        return merge.recursive({}, object);
    }
}

module.exports = Utils;