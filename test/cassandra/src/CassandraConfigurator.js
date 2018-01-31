const assert = require('assert');

const cassandraDriver = require('cassandra-driver');
const { policies: cassandraPolicies } = cassandraDriver;

const CassandraConfigurator = require('../../../cassandra/src/CassandraConfigurator');

describe('Cassandra Config', () => {
    it('Should create authentication provider instance (authProvider property) if username and password are defined', () => {
        const config = new CassandraConfigurator({
            username: 'test',
            password: 'test'
        }).configAuthProvider().config;

        assert.ok(config.authProvider instanceof cassandraDriver.auth.PlainTextAuthProvider);
    });

    it('Should create address resolution policy instance (policies.addressResolution) if address resolution policy type is defined', () => {
        const policies = {
            addressResolution: 'AddressTranslator'
        };
        const { AddressTranslator } = cassandraPolicies.addressResolution;

        const config = new CassandraConfigurator({ policies }).configAddressResolution().config;

        assert.ok(config.policies.addressResolution instanceof AddressTranslator);
    });

    it('Should create reconnection policy instance (policies.reconnection) if reconnection policy is defined', () => {
        const policies = {
            reconnection: {
                type: 'ExponentialReconnectionPolicy',
                params: {
                    baseDelay: 1,
                    maxDelay: 10,
                    startWithNoDelay: false
                }
            }
        };
        const { ExponentialReconnectionPolicy } = cassandraPolicies.reconnection;

        const config = new CassandraConfigurator({ policies }).configReconnection().config;

        assert.ok(config.policies.reconnection instanceof ExponentialReconnectionPolicy);
    });

    it('Should create speculative execution policy instance (policies.speculativeExecution) if speculative execution policy is defined', () => {
        const policies = {
            speculativeExecution: {
                type: 'ConstantSpeculativeExecutionPolicy',
                params: {
                    delay: 1,
                    maxSpeculativeExecutions: 10
                }
            }
        };
        const { ConstantSpeculativeExecutionPolicy } = cassandraPolicies.speculativeExecution;

        const config = new CassandraConfigurator({ policies }).configSpeculativeExecution().config;

        assert.ok(config.policies.speculativeExecution instanceof ConstantSpeculativeExecutionPolicy);
    });

    it('Should create timestamp generation policy instance (policies.timestampGeneration) if timestamp generation policy is defined', () => {
        const policies = {
            timestampGeneration: {
                type: 'MonotonicTimestampGenerator',
                params: {
                    warningThreshold: 100,
                    minLogInterval: 50
                }
            }
        };
        const { MonotonicTimestampGenerator } = cassandraPolicies.timestampGeneration;

        const config = new CassandraConfigurator({ policies }).configTimestampGeneration().config;

        assert.ok(config.policies.timestampGeneration instanceof MonotonicTimestampGenerator);
    });

    it('Should create wrapped load balancing policy instance (policies.loadBalancing) if load balancing policy is defined with childPolicy', () => {
        const policies = {
            loadBalancing: {
                type: 'WhiteListPolicy',
                params: {
                    childPolicy: {
                        type: 'DCAwareRoundRobinPolicy',
                        params: {
                            localDc: '127.0.0.1',
                            usedHostsPerRemoteDc: 2
                        }
                    },
                    whiteList: [ '127.0.0.1', '0.0.0.0' ]
                }
            }
        };
        const { WhiteListPolicy, DCAwareRoundRobinPolicy } = cassandraPolicies.loadBalancing;

        const config = new CassandraConfigurator({ policies }).configLoadBalancing().config;

        assert.ok(config.policies.loadBalancing instanceof WhiteListPolicy);
        assert.ok(config.policies.loadBalancing.childPolicy instanceof DCAwareRoundRobinPolicy);
    });

    it('Should create wrapped retry policy instance (policies.retry) if retry policy is defined with childPolicy', () => {
        const policies = {
            retry: {
                type: 'IdempotenceAwareRetryPolicy',
                params: {
                    childPolicy: {
                        type: 'RetryPolicy'
                    }
                }
            }
        };
        const { IdempotenceAwareRetryPolicy, RetryPolicy } = cassandraPolicies.retry;

        const config = new CassandraConfigurator({ policies }).configRetry().config;

        assert.ok(config.policies.retry instanceof IdempotenceAwareRetryPolicy);
        assert.ok(config.policies.retry._childPolicy instanceof RetryPolicy);
    });

    it('Should transform environment variable into config property format', () => {
        const propName = CassandraConfigurator.prepareConfigPropertyName('PROTOCOL_OPTIONS.PORT');
        assert.equal(propName, 'protocolOptions.port');
    });

    it('Should normalize environment variables config in format of camel cased nested object config', () => {
        const envConfig = {
            'PROTOCOL_OPTIONS': {
                'PORT': '9042'
            },
            'SOCKET_OPTIONS': {
                'KEEP_ALIVE': 'true',
                'CONNECT_TIMEOUT': '5000'
            },
            'CONTACT_POINTS': '127.0.0.1, 0.0.0.0'
        };

        const expectedConf = {
            protocolOptions: {
                port: '9042'
            },
            socketOptions: {
                keepAlive: true,
                connectTimeout: '5000'
            },
            contactPoints: [ '127.0.0.1', '0.0.0.0' ]
        };
        const normalizedConf = CassandraConfigurator.normalizeConfig(envConfig);

        assert.deepEqual(normalizedConf, expectedConf);
    });
});