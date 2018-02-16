const assert = require('assert');

const TableMetadataBuilder = require('../../dataBuilders/TableMetadataBuilder');

const JSONSchema = require('../../../../cassandra/lib/JSONSchema');

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

    it('Should return true if schema columns set is same as columns set in given metadata object', () => {
        const schema = new JSONSchema({
            id: 'int',
            __primaryKey__: [ 'id' ]
        });
        const metadata = new TableMetadataBuilder().withColumn('id').build();

        assert.equal(schema.comparePropertySetWithMetadata(metadata), true);
    });

    it('Should return false if schema columns set is NOT same as columns in given metadata object', () => {
        const schema = new JSONSchema({
            id: 'int',
            __primaryKey__: [ 'id' ]
        });
        const metadata = new TableMetadataBuilder().withColumn('col1').build();

        assert.equal(schema.comparePropertySetWithMetadata(metadata), false);
    });

    it('Should return true if schema primary key is same as primary key in given metadata object', () => {
        const schema = new JSONSchema({
            col1: 'int',
            col2: 'int',
            __primaryKey__: [ 'col1', 'col2' ]
        });
        const metadataBuilder = new TableMetadataBuilder().withIntColumn('col1');
        metadataBuilder.withIntColumn('col2').withPrimaryKey('col1', 'col2');
        const metadata = metadataBuilder.build();

        assert.equal(schema.comparePrimaryKeyWithMetadata(metadata), true);
    });

    it('Should return false if schema primary key is NOT same as primary key in given metadata object', () => {
        const schema = new JSONSchema({
            col1: 'int',
            col2: 'int',
            col3: 'int',
            __primaryKey__: [ 'col1', 'col3' ]
        });
        const metadataBuilder = new TableMetadataBuilder().withIntColumn('col1');
        metadataBuilder.withIntColumn('col2').withPrimaryKey('col1', 'col2');
        const metadata = metadataBuilder.build();

        assert.equal(schema.comparePrimaryKeyWithMetadata(metadata), false);
    });

    it('Should return array of mismatches in case some column types of schema do not match columns in metadata', () => {
        const schema = new JSONSchema({
            id: 'int',
            Col1: 'text',
            __primaryKey__: [ 'id' ]
        });
        const metadata = new TableMetadataBuilder().withTextColumn('id').withIntColumn('col1').build();

        const mismatches = schema.diffPropertyTypesWithMetadata(metadata);

        const expected = [
            {
                propName: 'id',
                realType: 'text',
                schemaType: 'int'
            },
            {
                propName: 'Col1',
                realType: 'int',
                schemaType: 'text'
            }
        ];
        assert.deepEqual(mismatches, expected);
    });

    it('Should NOT return any mismatches in case column types are map with same key and value types', () => {
        const schema = new JSONSchema({
            id: 'int',
            col1: 'map<text,text>',
            __primaryKey__: [ 'id' ]
        });
        const mdBuilder = new TableMetadataBuilder().withIntColumn('id').withMapColumn('col1');
        mdBuilder.withColumnNestedTextType('col1').withColumnNestedTextType('col1');
        const metadata = mdBuilder.build();

        const mismatches = schema.diffPropertyTypesWithMetadata(metadata);

        assert.equal(mismatches.length, 0);
    });

    it('Should NOT return any mismatches in case columns are the same user defined type', () => {
        const schema = new JSONSchema({
            id: 'int',
            col1: 'my_type',
            __primaryKey__: [ 'id' ]
        });

        const mdBuilder = new TableMetadataBuilder().withIntColumn('id').withUDTColumn('col1');
        mdBuilder.withColumnTypeName('col1', 'my_type');
        const metadata = mdBuilder.build();

        const mismatches = schema.diffPropertyTypesWithMetadata(metadata);

        assert.equal(mismatches.length, 0);
    });

    it('Should return mismatches in case columns are the same user defined type but in metadata type is frozen', () => {
        const schema = new JSONSchema({
            id: 'int',
            col1: 'my_type',
            __primaryKey__: [ 'id' ]
        });

        const mdBuilder = new TableMetadataBuilder().withIntColumn('id').withUDTColumn('col1');
        mdBuilder.withColumnTypeName('col1', 'my_type').withColumnTypeOption('col1', 'frozen', true);
        const metadata = mdBuilder.build();

        const mismatches = schema.diffPropertyTypesWithMetadata(metadata);

        assert.equal(mismatches.length, 1);
    });

    it('Should return mismatches in case columns are NOT the same user defined type', () => {
        const schema = new JSONSchema({
            id: 'int',
            col1: 'my_type',
            __primaryKey__: [ 'id' ]
        });

        const mdBuilder = new TableMetadataBuilder().withIntColumn('id').withUDTColumn('col1');
        mdBuilder.withColumnTypeName('col1', 'another_type');
        const metadata = mdBuilder.build();

        const mismatches = schema.diffPropertyTypesWithMetadata(metadata);

        assert.equal(mismatches.length, 1);
    });

    it('Should treat varchar as text in basic types and return 0 mismatches', () => {
        const schema = new JSONSchema({
            field1: 'varchar'
        });

        const mdBuilder = new TableMetadataBuilder().withTextColumn('field1');
        const metadata = mdBuilder.build();

        const mismatches = schema.diffPropertyTypesWithMetadata(metadata);

        assert.equal(mismatches.length, 0);
    });

    it('Should return false for shouldBeDropped() if __dropIfExists__ is absent in schema', () => {
        const schema = new JSONSchema({
            id: 'int',
            __primaryKey__: [ 'id' ]
        });

        assert.strictEqual(schema.shouldBeDropped(), false);
    });
});