const assert = require('assert');

const QueryBuilder = require('../../../cassandra/lib/QueryBuilder');

describe('Query Builder', () => {
    it('Should build query with schema matched data', () => {
        const schema = {
            col1: 'int',
            col2: 'int',
            __primaryKey__: [ 'col1' ],
            __clusteredKey__: [ 'col2' ]
        };
        const data = {
            col1: 1,
            col2: 2,
            col3: 3
        };

        const cql = new QueryBuilder().insertInto('table').queryParams(data).withJSONSchema(schema).build();

        assert.equal(cql.query, 'INSERT INTO table (col1, col2) VALUES (?, ?)');
        assert.deepEqual(cql.params, [ 1, 2 ]);
    });

    it('Should build query with UDT value', () => {
        const schema = {
            col1: 'int',
            col2: 'frozen<test_type>',
            __primaryKey__: [ 'col1' ]
        };
        const data = {
            col1: 1,
            col2: {
                prop1: 1,
                prop2: 'test'
            }
        };

        const cql = new QueryBuilder().insertInto('table').queryParams(data).withJSONSchema(schema).build();

        assert.equal(cql.query, 'INSERT INTO table (col1, col2) VALUES (?, ?)');
        assert.deepEqual(cql.params, [ 1, { prop1: 1, prop2: 'test' } ]);
    });

    it('Should cast to string values that are will be inserted in text, varchar or ascii column', () => {
        const schema = {
            col1: 'text',
            col2: 'varchar',
            col3: 'ascii'
        };
        const data = {
            col1: 123,
            col2: 123,
            col3: 123
        };

        const cql = new QueryBuilder().insertInto('table').queryParams(data).withJSONSchema(schema).build();

        assert.deepStrictEqual(cql.params, [ '123', '123', '123' ]);
    });

    it('Should fill values only once even if build() called multiple times', () => {
        const builder = new QueryBuilder().insertInto('table').queryParams({ val: 'test' });

        builder.build();
        builder.build();
        const cql = builder.build();

        assert.deepEqual(cql.params, [ 'test' ]);
    });
});