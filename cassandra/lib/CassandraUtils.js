const camelCase = require('camel-case');

const Utils = require('./Utils');

class CassandraUtils {
    /**
     * Create suitable config for cassandra-driver client from config with environmental variables style
     * @param config
     * @returns {Object}
     */
    static normalizeConfig(config) {
        const normalized = {};

        for (let key in config) {
            const propName = CassandraUtils.prepareConfigPropertyName(key);

            let value;
            if(Utils.isObject(config[key])) {
                value = CassandraUtils.normalizeConfig(config[key]);
            } else {
                value = config[key] && Utils.booleanOrDefault(config[key].toString().trim());
            }

            normalized[propName] = value;
        }

        if(typeof normalized.contactPoints === 'string') {
            normalized.contactPoints = normalized.contactPoints.split(',').map(point => point.trim());
        }

        return normalized;
    }

    /**
     * Formats property name in camel case
     * @param propName
     * @returns {string}
     */
    static prepareConfigPropertyName(propName) {
        return propName.split('.').map(camelCase).join('.');
    }

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