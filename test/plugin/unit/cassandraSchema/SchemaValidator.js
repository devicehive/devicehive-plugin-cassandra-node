const assert = require('assert');

const SchemaValidator = require('../../../../plugin/cassandraSchema/SchemaValidator');

describe('Schema Validator', () => {
    it('Should return error if parameters field is frozen list type', () => {
        const err = SchemaValidator.getSchemaError({
            parameters: 'frozen<list<int>>'
        });

        assert.notEqual(err, null);
    });

    it('Should return error if parameters field is tuple type', () => {
        const err = SchemaValidator.getSchemaError({
            parameters: 'tuple<double>'
        });

        assert.notEqual(err, null);
    });

    it('Should return error if parameters field is frozen set type', () => {
        const err = SchemaValidator.getSchemaError({
            parameters: 'frozen<set<int>>'
        });

        assert.notEqual(err, null);
    });

    it('Should return error if parameters field is timestamp', () => {
        const err = SchemaValidator.getSchemaError({
            parameters: 'timestamp'
        });

        assert.notEqual(err, null);
    });

    it('Should return error if parameters field is map with at least one non string component (key or value)', () => {
        const err = SchemaValidator.getSchemaError({
            parameters: 'map<int,text>'
        });

        assert.notEqual(err, null);
    });

    it('Should return error if parameters field is built in basic type and not a string', () => {
        const err = SchemaValidator.getSchemaError({
            parameters: 'int'
        });

        assert.notEqual(err, null);
    });

    it('Should return null if parameters field is text type', () => {
        const err = SchemaValidator.getSchemaError({
            parameters: 'text'
        });

        assert.equal(err, null);
    });

    it('Should return null if parameters field is ascii type', () => {
        const err = SchemaValidator.getSchemaError({
            parameters: 'ascii'
        });

        assert.equal(err, null);
    });

    it('Should return null if parameters field is varchar type', () => {
        const err = SchemaValidator.getSchemaError({
            parameters: 'varchar'
        });

        assert.equal(err, null);
    });

    it('Should return null if parameters field is frozen<map<text,text>> type', () => {
        const err = SchemaValidator.getSchemaError({
            parameters: 'frozen<map<text,text>>'
        });

        assert.equal(err, null);
    });

    it('Should return null if parameters field is frozen<map<text,text>> type', () => {
        const err = SchemaValidator.getSchemaError({
            parameters: 'frozen<map<text,text>>'
        });

        assert.equal(err, null);
    });

    it('Should return null if parameters field is UDT with frozen<>', () => {
        const err = SchemaValidator.getSchemaError({
            parameters: 'frozen<some_udt_with_int_>'
        });

        assert.equal(err, null);
    });

    it('Should return null if parameters field is UDT without frozen<>', () => {
        const err = SchemaValidator.getSchemaError({
            parameters: 'int_double_udt_detection_test'
        });

        assert.equal(err, null);
    });

    it('Should return empty array of errors in case there is no errors', () => {
        const tables = {
            tableA: {
                id: 'int'
            },
            tableB: {
                id: 'int'
            }
        };

        const errors = SchemaValidator.getSchemasErrors(tables);

        assert.equal(errors.length, 0);
    });
});