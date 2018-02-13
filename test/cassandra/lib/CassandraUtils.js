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

    it('Should cast value to string if Cassandra type is text, ascii or varchar', () => {
        assert.strictEqual(CassandraUtils.cassandraStringTypeOrDefault('text', 123), '123');
        assert.strictEqual(CassandraUtils.cassandraStringTypeOrDefault('ascii', 123), '123');
        assert.strictEqual(CassandraUtils.cassandraStringTypeOrDefault('varchar', 123), '123');
    });

    it('Should stringify to JSON object if Cassandra type is text, ascii or varchar', () => {
        const obj = { prop: 'test' };
        const stringified = JSON.stringify(obj);

        assert.strictEqual(CassandraUtils.cassandraStringTypeOrDefault('text', obj), stringified);
    });

    it('Should return null if second argument (value) for cassandraStringTypeOrDefault is null or undefined', () => {
        assert.strictEqual(CassandraUtils.cassandraStringTypeOrDefault('text'), null);
        assert.strictEqual(CassandraUtils.cassandraStringTypeOrDefault('text', null), null);
    });

    it('Should replace "varchar" alias with "text"', () => {
        const simpleType = 'varchar';
        const complexType = 'frozen<map<varchar,varchar>>';

        assert.equal(CassandraUtils.replaceTypeAliases(simpleType), 'text');
        assert.equal(CassandraUtils.replaceTypeAliases(complexType), 'frozen<map<text,text>>');
    });
});