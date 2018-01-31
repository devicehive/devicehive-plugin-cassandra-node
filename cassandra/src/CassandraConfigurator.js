const cassandraDriver = require('cassandra-driver');
const camelCase = require('camel-case');

const Utils = require('../lib/Utils');

class CassandraConfigurator {
    static get RECONNECTION_POLICY() { return 'reconnection'; }
    static get SPECULATIVE_EXEC_POLICY() { return 'speculativeExecution'; }
    static get TIMESTAMP_GENERATION_POLICY() { return 'timestampGeneration'; }
    static get LOAD_BALANCING_POLICY() { return 'loadBalancing'; }
    static get RETRY_POLICY() { return 'retry'; }

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
        return this.configPolicy(CassandraConfigurator.RECONNECTION_POLICY);
    }

    configSpeculativeExecution() {
        return this.configPolicy(CassandraConfigurator.SPECULATIVE_EXEC_POLICY);
    }

    configTimestampGeneration() {
        return this.configPolicy(CassandraConfigurator.TIMESTAMP_GENERATION_POLICY);
    }

    configLoadBalancing() {
        return this.configPolicy(CassandraConfigurator.LOAD_BALANCING_POLICY);
    }

    configRetry() {
        return this.configPolicy(CassandraConfigurator.RETRY_POLICY);
    }

    configPolicy(name, orderedParamNames = []) {
        if (this.isEmptyPolicy(name)) {
            return this;
        }

        const policyConfig = this._config.policies[name];
        this._config.policies[name] = this._createPolicyInstance(name, policyConfig, orderedParamNames);

        return this;
    }

    _createPolicyInstance(policyName, instanceDescriptor) {
        let className = instanceDescriptor;
        let params;
        let args = [];
        if (instanceDescriptor.type) {
            className = instanceDescriptor.type;

            const orderedParamNames = CassandraConfigurator._getOrderedConstructorParams(policyName, className) || [];
            params = instanceDescriptor.params || {};
            args = orderedParamNames.map(paramName => {
                const isInstanceDescriptor = Utils.isObject(params[paramName]) && 'type' in params[paramName];
                if (isInstanceDescriptor) {
                    return this._createPolicyInstance(policyName, params[paramName]);
                }

                return params[paramName];
            });
        }

        const PolicyClass = cassandraDriver.policies[policyName][className];

        return new PolicyClass(...args);
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

        if (typeof normalized.contactPoints === 'string') {
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

    static _getOrderedConstructorParams(policy, className) {
        const policyClassesParams = orderedConstructorParams[policy];
        return policyClassesParams ? policyClassesParams[className] : [];
    }
}

const orderedConstructorParams = {
    [CassandraConfigurator.RECONNECTION_POLICY]: {
        ReconnectionPolicy: [],
        ConstantReconnectionPolicy: [ 'delay' ],
        ExponentialReconnectionPolicy: [ 'baseDelay', 'maxDelay', 'startWithNoDelay' ]
    },

    [CassandraConfigurator.SPECULATIVE_EXEC_POLICY]: {
        SpeculativeExecutionPolicy: [],
        NoSpeculativeExecutionPolicy: [],
        ConstantSpeculativeExecutionPolicy: [ 'delay', 'maxSpeculativeExecutions' ]
    },

    [CassandraConfigurator.TIMESTAMP_GENERATION_POLICY]: {
        TimestampGenerator: [],
        MonotonicTimestampGenerator: [ 'warningThreshold', 'minLogInterval' ]
    },

    [CassandraConfigurator.LOAD_BALANCING_POLICY]: {
        LoadBalancingPolicy: [],
        RoundRobinPolicy: [],
        DCAwareRoundRobinPolicy: [ 'localDc', 'usedHostsPerRemoteDc' ],
        TokenAwarePolicy: [ 'childPolicy' ],
        WhiteListPolicy: [ 'childPolicy', 'whiteList' ]
    },

    [CassandraConfigurator.RETRY_POLICY]: {
        RetryPolicy: [],
        IdempotenceAwareRetryPolicy: [ 'childPolicy' ]
    }
};

module.exports = CassandraConfigurator;