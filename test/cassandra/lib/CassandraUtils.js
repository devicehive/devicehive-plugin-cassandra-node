const assert = require('assert');

const CassandraUtils = require('../../../cassandra/lib/CassandraUtils');

describe('Cassandra Utils', () => {
    it('Should transform environment variable into config property format', () => {
        const propName = CassandraUtils.prepareConfigPropertyName('PROTOCOL_OPTIONS.PORT');
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
        const normalizedConf = CassandraUtils.normalizeConfig(envConfig);

        assert.deepEqual(normalizedConf, expectedConf);
    });

    it('Should extract UDT name from column type with frozen<*>', () => {
        const typeName = CassandraUtils.extractTypeName('frozen<test_custom_type>');
        assert.equal(typeName, 'test_custom_type');
    });

    it('Should extract UDT name from column type with FROZEN<*>', () => {
        const typeName = CassandraUtils.extractTypeName('FROZEN<test_custom_type>');
        assert.equal(typeName, 'test_custom_type');
    });
});