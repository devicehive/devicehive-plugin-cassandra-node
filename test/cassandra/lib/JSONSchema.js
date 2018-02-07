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

    it('Should return null in case object to be filtered is null or undefined', () => {
        const jsonSchema = new JSONSchema({
            col1: 'text'
        });

        assert.equal(jsonSchema.filterData(), null);
        assert.equal(jsonSchema.filterData(null), null);
    });

    it('Should cast value to string if Cassandra type is text, ascii or varchar', () => {
        assert.strictEqual(JSONSchema.cassandraStringTypeOrDefault('text', 123), '123');
        assert.strictEqual(JSONSchema.cassandraStringTypeOrDefault('ascii', 123), '123');
        assert.strictEqual(JSONSchema.cassandraStringTypeOrDefault('varchar', 123), '123');
    });

    it('Should stringify to JSON object if Cassandra type is text, ascii or varchar', () => {
        const obj = { prop: 'test' };
        const stringified = JSON.stringify(obj);

        assert.strictEqual(JSONSchema.cassandraStringTypeOrDefault('text', obj), stringified);
    });

    it('Should return null if second argument (value) for cassandraStringTypeOrDefault is null or undefined', () => {
        assert.strictEqual(JSONSchema.cassandraStringTypeOrDefault('text'), null);
        assert.strictEqual(JSONSchema.cassandraStringTypeOrDefault('text', null), null);
    });

    it('Should return filtered object consisted of primary and clustered keys only', () => {
        const schema = new JSONSchema({
            id: 'int',
            clustered: 'int',
            col1: 'text',

            __primaryKey__: [ 'id' ],
            __clusteredKey__: [ 'clustered' ]
        });
        const data = {
            id: 123,
            col1: 'test',
            clustered: 111
        };

        const keyValues = schema.extractKeys(data);

        assert.deepEqual(keyValues, {
            id: 123,
            clustered: 111
        });
    });

    it('Should return filtered object consisted of not key columns only', () => {
        const schema = new JSONSchema({
            id: 'int',
            clustered: 'int',
            col1: 'text',
            col2: 'text',

            __primaryKey__: [ 'id' ],
            __clusteredKey__: [ 'clustered' ]
        });
        const data = {
            id: 123,
            col1: 'test',
            col2: 'test2',
            clustered: 111
        };

        const notKeyValues = schema.extractNotKeys(data);

        assert.deepEqual(notKeyValues, {
            col1: 'test',
            col2: 'test2'
        });
    });
});