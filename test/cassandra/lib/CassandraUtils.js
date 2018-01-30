const assert = require('assert');

const CassandraUtils = require('../../../cassandra/lib/CassandraUtils');

describe('Cassandra Utils', () => {
    it('Should extract UDT name from column type with frozen<*>', () => {
        const typeName = CassandraUtils.extractTypeName('frozen<test_custom_type>');
        assert.equal(typeName, 'test_custom_type');
    });

    it('Should extract UDT name from column type with FROZEN<*>', () => {
        const typeName = CassandraUtils.extractTypeName('FROZEN<test_custom_type>');
        assert.equal(typeName, 'test_custom_type');
    });
});