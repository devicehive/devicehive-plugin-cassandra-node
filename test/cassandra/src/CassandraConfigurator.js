const assert = require('assert');

const CassandraConfigurator = require('../../../cassandra/src/CassandraConfigurator');

describe('Cassandra Config', () => {
    it('Should create authentication provider instance (authProvider property) if username and password are defined', () => {
        const conf = new CassandraConfigurator({
            username: 'test',
            password: 'test'
        }).configAuthProvider();

        assert.equal(typeof conf.config.authProvider, 'object');
    });

    it('Should create address resolution policy instance (policies.addressResolution) if address resolution policy type is defined', () => {
        const conf = new CassandraConfigurator({
            policies: {
                addressResolution: 'AddressTranslator'
            }
        }).configAddressResolution();

        assert.equal(typeof conf.config.policies.addressResolution, 'object');
    });

    it('Should create reconnection policy instance (policies.reconnection) if reconnection policy type is defined', () => {
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
        const conf = new CassandraConfigurator({ policies }).configReconnection();

        assert.equal(typeof conf.config.policies.reconnection, 'object');
    });

    it('Should create speculative execution policy instance (policies.speculativeExecution) if speculative execution policy type is defined', () => {
        const policies = {
            speculativeExecution: {
                type: 'ConstantSpeculativeExecutionPolicy',
                params: {
                    delay: 1,
                    maxSpeculativeExecutions: 10
                }
            }
        };
        const conf = new CassandraConfigurator({ policies }).configSpeculativeExecution();

        assert.equal(typeof conf.config.policies.speculativeExecution, 'object');
    });

    it('Should create timestamp generation policy instance (policies.timestampGeneration) if timestamp generation policy type is defined', () => {
        const policies = {
            timestampGeneration: {
                type: 'MonotonicTimestampGenerator',
                params: {
                    warningThreshold: 100,
                    minLogInterval: 50
                }
            }
        };
        const conf = new CassandraConfigurator({ policies }).configTimestampGeneration();

        assert.equal(typeof conf.config.policies.timestampGeneration, 'object');
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