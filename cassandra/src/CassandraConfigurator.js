const cassandraDriver = require('cassandra-driver');
const camelCase = require('camel-case');

const Utils = require('../lib/Utils');

class CassandraConfigurator {
    constructor(config) {
        this._config = Utils.copy(config);
    }

    get config() {
        return this._config;
    }

    configAuthProvider() {
        const { username, password } = this._config;

        if (username && password) {
            this._config.authProvider = new cassandraDriver.auth.PlainTextAuthProvider(username, password);
        }

        delete this._config.username;
        delete this._config.password;

        return this;
    }

    configAddressResolution() {
        return this.configPolicy('addressResolution');
    }

    configReconnection() {
        return this.configPolicy('reconnection', [ 'baseDelay', 'maxDelay', 'startWithNoDelay' ]);
    }

    configSpeculativeExecution() {
        return this.configPolicy('speculativeExecution', [ 'delay', 'maxSpeculativeExecutions' ]);
    }

    configTimestampGeneration() {
        return this.configPolicy('timestampGeneration', [ 'warningThreshold', 'minLogInterval' ]);
    }

    configPolicy(name, paramNames) {
        if (this.isEmptyPolicy(name)) {
            return this;
        }

        const policyConfig = this._config.policies[name];

        let className = policyConfig;
        let params;
        let args = [];
        if (policyConfig.type) {
            className = policyConfig.type;
            params = policyConfig.params || {};
            args = paramNames.map(pName => params[pName]);
        }

        const PolicyClass = cassandraDriver.policies[name][className];
        this._config.policies[name] = new PolicyClass(...args);

        return this;
    }

    isEmptyPolicy(name) {
        return !this._config.policies || !this._config.policies[name];
    }

    /**
     * Create suitable config for cassandra-driver client from config with environmental variables style
     * @param config
     * @returns {Object}
     */
    static normalizeConfig(config) {
        const normalized = {};

        for (let key in config) {
            const propName = CassandraConfigurator.prepareConfigPropertyName(key);

            let value;
            if(Utils.isObject(config[key])) {
                value = CassandraConfigurator.normalizeConfig(config[key]);
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
}

module.exports = CassandraConfigurator;