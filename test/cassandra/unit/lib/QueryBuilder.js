const assert = require('assert');

const QueryBuilder = require('../../../../cassandra/lib/QueryBuilder');

describe('Query Builder', () => {
    it('Should build query with schema matched data', () => {
        const schema = {
            col1: 'int',
            col2: 'int',
            __primaryKey__: [ 'col1' ],
            __clusteringKey__: [ 'col2' ]
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

    it('Should build update query with IF EXISTS', () => {
        const builder = new QueryBuilder().update('table').where('key1=?', 'test').where('key2=?', '2test2');
        builder.queryParams({ val: 'set value', val2: 'set value 2' });

        const cql = builder.build();

        assert.equal(cql.query, 'UPDATE table SET val = ?, val2 = ? WHERE key1=? AND key2=? IF EXISTS');
        assert.deepEqual(cql.params, [ 'set value', 'set value 2', 'test', '2test2' ]);
    });

    it('Should construct WHERE condition based on JSON schema for UPDATE query', () => {
        const schema = {
            col1: 'text',
            col2: 'int',
            id: 'int',
            id2: 'int',
            clustering: 'int',
            __primaryKey__: [ 'id', 'id2' ],
            __clusteringKey__: [ 'clustering' ]
        };
        const data = {
            col1: 'test',
            col2: 111,
            id: 1,
            id2: 2,
            clustering: 3
        };

        const builder = new QueryBuilder().update('table').withJSONSchema(schema).queryParams(data);

        const cql = builder.build();

        assert.equal(cql.query, 'UPDATE table SET col1 = ?, col2 = ? WHERE id = ? AND id2 = ? AND clustering = ? IF EXISTS')
        assert.deepEqual(cql.params, [ 'test', 111, 1, 2, 3 ]);
    });
});