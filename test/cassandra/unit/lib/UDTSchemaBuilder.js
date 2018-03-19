const assert = require('assert');

const UDTSchemaBuilder = require('../../../../cassandra/lib/UDTSchemaBuilder');

describe('User Defined Type Builder', () => {
    it('Should build query for UDT creation', () => {
        const builder = new UDTSchemaBuilder().createType('test');

        builder.fromJSONSchema({
            prop1: 'int',
            prop2: 'text',
            prop3: 'timestamp'
        });
        const query = builder.build();

        assert.equal(query, 'CREATE TYPE test(prop1 int,prop2 text,prop3 timestamp)');
    });

    it('Should build query for UDT creation with IF NOT EXISTS', () => {
        const builder = new UDTSchemaBuilder().createType('test');

        builder.fromJSONSchema({
            prop1: 'int'
        });
        const query = builder.ifNotExists().build();

        assert.equal(query, 'CREATE TYPE IF NOT EXISTS test(prop1 int)');
    });

    it('Should build DROP TYPE query', () => {
        const query = new UDTSchemaBuilder().dropType('test').build();
        assert.equal(query, 'DROP TYPE test');
    });

    it('Should build DROP TABLE query with IF EXISTS', () => {
        const query = new UDTSchemaBuilder().dropType('test').ifExists().build();
        assert.equal(query, 'DROP TYPE IF EXISTS test');
    });
});