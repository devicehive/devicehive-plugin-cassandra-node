const assert = require('assert');

const JSONSchema = require('../../../cassandra/lib/JSONSchema');

describe('JSON Schema', () => {
    it('Should filter data object based on table schema', () => {
        const object = {
            col1: 123,
            col2: 456,
            col3: 'some-text',
            col4: 'to be filtered'
        };
        const jsonSchema = new JSONSchema({
            col1: 'int',
            col3: 'text'
        });

        const filteredObject = jsonSchema.filterData(object);

        assert.deepEqual(filteredObject, {
            col1: 123,
            col3: 'some-text'
        });
    });

    it('Should omit properties which exist in schema but do not exist in data object', () => {
        const object = {
            col1: 123,
            col2: 456
        };
        const jsonSchema = new JSONSchema({
            col1: 'int',
            col3: 'text'
        });

        const filteredObject = jsonSchema.filterData(object);

        assert.deepEqual(filteredObject, {
            col1: 123
        });
    });

    it('Should filter data with user defined types', () => {
        const object = {
            col1: 123,
            custom: {
                prop1: 123,
                prop2: 'to be filtered',
                prop3: 'to be included',
                prop4: 'to be filtered'
            }
        };
        const jsonSchema = new JSONSchema({
            col1: 'int',
            custom: 'frozen<customType>'
        }).fillWithTypes({
            customType: {
                prop1: 'int',
                prop3: 'text'
            }
        });

        const filteredObject = jsonSchema.filterData(object);

        assert.deepEqual(filteredObject, {
            col1: 123,
            custom: {
                prop1: 123,
                prop3: 'to be included'
            }
        });
    });

    it('Should cast value to string if Cassandra type is text, ascii or varchar', () => {
        assert.strictEqual(JSONSchema.cassandraStringTypeOrDefault('text', 123), '123');
        assert.strictEqual(JSONSchema.cassandraStringTypeOrDefault('ascii', 123), '123');
        assert.strictEqual(JSONSchema.cassandraStringTypeOrDefault('varchar', 123), '123');
    });
});