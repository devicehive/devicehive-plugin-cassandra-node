const assert = require('assert');

const TableSchemaBuilder = require('../../../../cassandra/lib/TableSchemaBuilder');

describe('Table Schema Builder', () => {
    it('Should build query for table creation with specified fields', () => {
        const tableName = 'my_table';
        const tableSchema = {
            textField: 'text',
            bigIntField: 'bigint',
            floatField: 'float',
            __primaryKey__: [ 'textField', 'bigIntField' ],
            __clusteringKey__: [ 'floatField' ]
        };
        const builder = new TableSchemaBuilder();

        builder.createTable(tableName).fromJSONSchema(tableSchema);

        const query = builder.build();
        assert.equal(query, 'CREATE TABLE my_table(textField text,bigIntField bigint,floatField float,PRIMARY KEY((textField,bigIntField),floatField))');
    });

    it('Should build query without clustering key if it does not specified', () => {
        const tableSchema = {
            textField: 'text',
            bigIntField: 'bigint',
            __primaryKey__: [ 'bigIntField' ]
        };

        const builder = new TableSchemaBuilder();
        builder.createTable('my_table').fromJSONSchema(tableSchema);
        const query = builder.build();

        assert.equal(query, 'CREATE TABLE my_table(textField text,bigIntField bigint,PRIMARY KEY((bigIntField)))');
    });

    it('Should build query without clustering key if it is zero-length property', () => {
        const tableSchema = {
            textField: 'text',
            bigIntField: 'bigint',
            __primaryKey__: [ 'bigIntField' ],
            __clusteringKey__: []
        };

        const builder = new TableSchemaBuilder();
        builder.createTable('my_table').fromJSONSchema(tableSchema);

        const query = builder.build();
        assert.equal(query, 'CREATE TABLE my_table(textField text,bigIntField bigint,PRIMARY KEY((bigIntField)))');
    });

    it('Should build query for table creation with IF NOT EXISTS', () => {
        const tableSchema = {
            col1: 'int',
            __primaryKey__: [ 'col1' ]
        };

        const builder = new TableSchemaBuilder().createTable('my_table').fromJSONSchema(tableSchema).ifNotExists();

        const query = builder.build();
        assert.equal(query, 'CREATE TABLE IF NOT EXISTS my_table(col1 int,PRIMARY KEY((col1)))');
    });

    it('Should build query for table creation with ordering definition', () => {
        const tableSchema = {
            col1: 'int',
            col2: 'int',
            col3: 'int',
            __primaryKey__: [ 'col1' ],
            __clusteringKey__: [ 'col2', 'col3' ],
            __order__: {
                col2: 'ASC',
                col3: 'DESC'
            }
        };

        const builder = new TableSchemaBuilder().createTable('my_table').fromJSONSchema(tableSchema);

        const query = builder.build();
        assert.equal(query, 'CREATE TABLE my_table(col1 int,col2 int,col3 int,PRIMARY KEY((col1),col2,col3)) WITH CLUSTERING ORDER BY(col2 ASC,col3 DESC)');
    });

    it('Should build query for table creation with table options', () => {
        const tableSchema = {
            col1: 'int',
            __primaryKey__: [ 'col1' ],
            __options__: {
                bloom_filter_fp_chance: 0.1,
                comment: '',
                compaction: {
                    'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy',
                    'max_threshold': 32,
                    'min_threshold': 4
                }
            }
        };

        const builder = new TableSchemaBuilder().createTable('my_table').fromJSONSchema(tableSchema);

        let expectedQuery = `CREATE TABLE my_table(col1 int,PRIMARY KEY((col1)))`;
        expectedQuery = `${expectedQuery} WITH bloom_filter_fp_chance = 0.1 AND comment = ''`;
        expectedQuery = `${expectedQuery} AND compaction = {'class':'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy','max_threshold':32,'min_threshold':4}`;

        const query = builder.build();
        assert.equal(query, expectedQuery);
    });

    it('Should build query for table creation with table options AND ordering', () => {
        const tableSchema = {
            col1: 'int',
            col2: 'int',
            __primaryKey__: [ 'col1' ],
            __clusteringKey__: [ 'col2' ],
            __options__: {
                bloom_filter_fp_chance: 0.1
            },
            __order__: {
                col2: 'DESC'
            }
        };

        const builder = new TableSchemaBuilder().createTable('my_table').fromJSONSchema(tableSchema);

        let expectedQuery = `CREATE TABLE my_table(col1 int,col2 int,PRIMARY KEY((col1),col2))`;
        expectedQuery = `${expectedQuery} WITH CLUSTERING ORDER BY(col2 DESC)`;
        expectedQuery = `${expectedQuery} AND bloom_filter_fp_chance = 0.1`;

        const query = builder.build();
        assert.equal(query, expectedQuery);
    });

    it('Should build DROP TABLE query', () => {
        const query = new TableSchemaBuilder().dropTable('my_table').build();
        assert.equal(query, 'DROP TABLE my_table');
    });

    it('Should build DROP TABLE query with IF EXISTS', () => {
        const query = new TableSchemaBuilder().dropTable('my_table').ifExists().build();
        assert.equal(query, 'DROP TABLE IF EXISTS my_table');
    });
});