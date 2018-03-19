const assert = require('assert');

const cassandraDriver = require('cassandra-driver');

const TEST_KEYSPACE = 'plugin_schema_creation_integration_tests';
const CASSANDRA_CONFIG = {
    CONNECTION: {
        CONTACT_POINTS: '127.0.0.1',
        KEYSPACE: TEST_KEYSPACE
    }
};
const CASSANDRA_DRIVER_CONFIG = {
    contactPoints: [ '127.0.0.1' ]
};

describe('Integration tests: Schema Creation', function() {
    this.timeout(5000);

    let createSchema;
    const cassandraDriverClient = new cassandraDriver.Client(CASSANDRA_DRIVER_CONFIG);

    before(async () => {
        await cassandraDriverClient.connect();
    });

    beforeEach(async () => {
        await dropTestKeyspace(cassandraDriverClient);
        await createTestKeyspace(cassandraDriverClient);

        delete require.cache[require.resolve('../../../../plugin/cassandraSchema/createSchema')];
        createSchema = require('../../../../plugin/cassandraSchema/createSchema');
    });

    after(async () => {
        await dropTestKeyspace(cassandraDriverClient);
    });

    it('Should create schema', async () => {
        const tables = {
            mytable: {
                id: 'int',
                __primaryKey__: ['id']
            },
            mytable1: {
                id: 'int',
                __primaryKey__: ['id']
            }
        };
        const udts = {
            mytype: {
                prop: 'int'
            },
            mytype1: {
                prop: 'int'
            }
        };

        await createSchema(CASSANDRA_CONFIG, udts, tables);

        const [ myTable, myTable1, myType, myType1 ] = await Promise.all([
            cassandraDriverClient.metadata.getTable(TEST_KEYSPACE, 'mytable'),
            cassandraDriverClient.metadata.getTable(TEST_KEYSPACE, 'mytable1'),
            cassandraDriverClient.metadata.getUdt(TEST_KEYSPACE, 'mytype'),
            cassandraDriverClient.metadata.getUdt(TEST_KEYSPACE, 'mytype1')
        ]);

        assert.notEqual(myTable, null);
        assert.notEqual(myTable1, null);
        assert.notEqual(myType, null);
        assert.notEqual(myType1, null);
    });

    it('Should drop marked schemas with __dropIfExists__ before creation', async () => {
        const tables = {
            mytable: {
                b: 'int',
                __primaryKey__: [ 'b' ],
                __dropIfExists__: true
            }
        };
        const udts = {
            mytype: {
                b: 'int',
                __dropIfExists__: true
            }
        };

        await cassandraDriverClient.execute('CREATE TABLE mytable(a int PRIMARY KEY)');
        await cassandraDriverClient.execute('CREATE TYPE mytype(a int)');

        await createSchema(CASSANDRA_CONFIG, udts, tables);

        const [ udt, table ] = await Promise.all([
            cassandraDriverClient.metadata.getUdt(TEST_KEYSPACE, 'mytype'),
            cassandraDriverClient.metadata.getTable(TEST_KEYSPACE, 'mytable')
        ]);

        assert.notEqual(udt, null);
        assert.notEqual(table, null);
        assert.equal(udt.fields[0].name, 'b');
        assert.notEqual(table.columnsByName.b, undefined);
    });

    it('Should fail with error in case of table columns set mismatch', done => {
        const tables = {
            mytable: {
                col1: 'int',
                __primaryKey__: [ 'col1' ]
            }
        };

        cassandraDriverClient.execute('CREATE TABLE mytable(col1 int PRIMARY KEY, col2 int)').then(() => {
            return createSchema(CASSANDRA_CONFIG, {}, tables);
        }).then(() => {
            done(new Error('Does not throw SchemaError because of columns set mismatch'));
        }).catch(() => {
            done();
        });
    });

    it('Should fail with error in case of table column types mismatch', done => {
        const tables = {
            mytable: {
                col1: 'int',
                __primaryKey__: [ 'col1' ]
            }
        };

        cassandraDriverClient.execute('CREATE TABLE mytable(col1 text PRIMARY KEY)').then(() => {
            return createSchema(CASSANDRA_CONFIG, {}, tables);
        }).then(() => {
            done(new Error('Does not throw SchemaError because of column types mismatch'));
        }).catch(err => {
            done();
        });
    });

    it('Should fail with error in case of type fields set mismatch', done => {
        const udts = {
            mytype: {
                field1: 'int'
            }
        };

        cassandraDriverClient.execute('CREATE TYPE mytype(field1 int, field2 int)').then(() => {
            return createSchema(CASSANDRA_CONFIG, udts, {});
        }).then(() => {
            done(new Error('Does not throw SchemaError because of fields set mismatch'));
        }).catch(err => {
            done();
        });
    });

    it('Should fail with error in case of type field types mismatch', done => {
        const udts = {
            mytype: {
                field1: 'int'
            }
        };

        cassandraDriverClient.execute('CREATE TYPE mytype(field1 text)').then(() => {
            return createSchema(CASSANDRA_CONFIG, udts, {});
        }).then(() => {
            done(new Error('Does not throw SchemaError because of field types mismatch'));
        }).catch(err => {
            done();
        });
    });
});

function dropTestKeyspace(cassandraDriverClient) {
    return cassandraDriverClient.execute(`DROP KEYSPACE IF EXISTS ${TEST_KEYSPACE}`);
}

async function createTestKeyspace(cassandraDriverClient) {
    await cassandraDriverClient.execute(`CREATE KEYSPACE ${TEST_KEYSPACE} WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 }`);
    return cassandraDriverClient.execute(`USE ${TEST_KEYSPACE}`);
}