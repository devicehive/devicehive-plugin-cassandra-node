const assert = require('assert');

const cassandraDriver = require('cassandra-driver');

const CassandraStorage = require('../../../cassandra/src/CassandraStorage');

const TEST_KEYSPACE = 'integration_tests';
const CONFIG = {
    contactPoints: [ '127.0.0.1' ]
};

describe('Integration tests: Cassandra Storage', function() {
    this.timeout(5000);

    let cassandraDriverClient = new cassandraDriver.Client(CONFIG);
    let cassandraStorage;

    before(async () => {
        await cassandraDriverClient.connect();
    });

    beforeEach(async () => {
        cassandraStorage = new CassandraStorage(cassandraDriverClient);
        await dropTestKeyspace(cassandraDriverClient);
        await createTestKeyspace(cassandraDriverClient);
    });

    after(async () => {
        await dropTestKeyspace(cassandraDriverClient);
    });

    it('Should create table with given JSON schema', async () => {
        const tableSchemas = {
            test: {
                col1: 'int',
                col2: 'text',
                __primaryKey__: [ 'col1' ]
            }
        };

        const res = await cassandraStorage.createTableSchemas(tableSchemas);
        assert.notEqual(res, null);

        const metadata = await cassandraDriverClient.metadata.getTable(TEST_KEYSPACE, 'test');
        assert.notEqual(metadata, null);
        assert.equal(metadata.columns.length, 2, 'Contains all columns described in schema');
        assert.equal(metadata.partitionKeys.length, 1);
        assert.equal(metadata.partitionKeys[0].name, 'col1');
    });

    it('Should create UDT with given JSON schema', async () => {
        const udtSchemas = {
            test: {
                field1: 'int',
                field2: 'int'
            }
        };

        const res = await cassandraStorage.createUDTSchemas(udtSchemas);
        assert.notEqual(res, null);

        const metadata = await cassandraDriverClient.metadata.getUdt(TEST_KEYSPACE, 'test');
        assert.notEqual(metadata, null);
        assert.equal(metadata.fields.length, 2, 'Contains all fields described in schema');
    });

    it('Should drop table which has __dropIfExists__ as true in schema', async () => {
        const tableSchemas = {
            test: {
                col1: 'int',
                __primaryKey__: [ 'col1' ],
                __dropIfExists__: true
            },

            test2: {
                col2: 'int',
                __primaryKey__: [ 'col2' ]
            }
        };
        
        await cassandraDriverClient.execute(`CREATE TABLE ${TEST_KEYSPACE}.test(col1 int PRIMARY KEY)`);
        await cassandraDriverClient.execute(`CREATE TABLE ${TEST_KEYSPACE}.test2(col2 int PRIMARY KEY)`);

        await cassandraStorage.dropTableSchemas(tableSchemas);

        let testTableMetadata = await cassandraDriverClient.metadata.getTable(TEST_KEYSPACE, 'test');
        let testTable2Metadata = await cassandraDriverClient.metadata.getTable(TEST_KEYSPACE, 'test2');
        assert.equal(testTableMetadata, null);
        assert.notEqual(testTable2Metadata, null);
    });

    it('Should drop type which has __dropIfExists__ as true in schema', async () => {
        const typeSchemas = {
            test: {
                field1: 'int',
                __dropIfExists__: true
            },

            test2: {
                field2: 'int'
            }
        };
        
        await cassandraDriverClient.execute(`CREATE TYPE ${TEST_KEYSPACE}.test(field1 int)`);
        await cassandraDriverClient.execute(`CREATE TYPE ${TEST_KEYSPACE}.test2(field2 int)`);

        await cassandraStorage.dropTypeSchemas(typeSchemas);

        let testUdtMetadata = await cassandraDriverClient.metadata.getUdt(TEST_KEYSPACE, 'test');
        let testUdt2Metadata = await cassandraDriverClient.metadata.getUdt(TEST_KEYSPACE, 'test2');
        assert.equal(testUdtMetadata, null);
        assert.notEqual(testUdt2Metadata, null);
    });

    it('Should insert command', async () => {
        const tableSchemas = {
            test: {
                command: 'text',
                __primaryKey__: [ 'command' ]
            }
        };

        await cassandraDriverClient.execute(`CREATE TABLE ${TEST_KEYSPACE}.test(command text PRIMARY KEY)`);
        cassandraStorage.setTableSchemas(tableSchemas).assignTablesToCommands([ 'test' ]);
        
        await cassandraStorage.insertCommand({
            command: 'test command'
        });

        const { rows: insertedRows } = await cassandraDriverClient.execute(`SELECT * FROM ${TEST_KEYSPACE}.test`);
        assert.equal(insertedRows.length, 1);
    });

    it('Should insert notification', async () => {
        const tableSchemas = {
            test: {
                notification: 'text',
                __primaryKey__: [ 'notification' ]
            }
        };

        await cassandraDriverClient.execute(`CREATE TABLE ${TEST_KEYSPACE}.test(notification text PRIMARY KEY)`);
        cassandraStorage.setTableSchemas(tableSchemas).assignTablesToNotifications([ 'test' ]);
        
        await cassandraStorage.insertNotification({
            notification: 'test notification'
        });

        const { rows: insertedRows } = await cassandraDriverClient.execute(`SELECT * FROM ${TEST_KEYSPACE}.test`);
        assert.equal(insertedRows.length, 1);
    });

    it('Should insert command update', async () => {
        const tableSchemas = {
            test: {
                command: 'text',
                __primaryKey__: [ 'command' ]
            }
        };

        await cassandraDriverClient.execute(`CREATE TABLE ${TEST_KEYSPACE}.test(command text PRIMARY KEY)`);
        cassandraStorage.setTableSchemas(tableSchemas).assignTablesToCommandUpdates([ 'test' ]);
        
        await cassandraStorage.insertCommandUpdate({
            command: 'test command update'
        });

        const { rows: insertedRows } = await cassandraDriverClient.execute(`SELECT * FROM ${TEST_KEYSPACE}.test`);
        assert.equal(insertedRows.length, 1);
    });

    it('Should update command', async () => {
        const tableSchemas = {
            test: {
                command: 'text',
                deviceId: 'text',
                __primaryKey__: [ 'command' ]
            }
        };

        await cassandraDriverClient.execute(`CREATE TABLE ${TEST_KEYSPACE}.test(command text PRIMARY KEY, deviceId text)`);
        await cassandraDriverClient.execute(`INSERT INTO ${TEST_KEYSPACE}.test(command, deviceId) VALUES('test command', 'deviceId')`);
        cassandraStorage.setTableSchemas(tableSchemas).assignTablesToCommands([ 'test' ]);
        
        await cassandraStorage.updateCommand({
            command: 'test command',
            deviceId: 'updated'
        });

        const { rows: updatedRows } = await cassandraDriverClient.execute(`SELECT * FROM ${TEST_KEYSPACE}.test`);
        assert.equal(updatedRows.length, 1);
        assert.equal(updatedRows[0].deviceid, 'updated');
    });

    it('Should update command only if it exists', async () => {
        const tableSchemas = {
            test: {
                command: 'text',
                deviceId: 'text',
                __primaryKey__: [ 'command' ]
            }
        };

        await cassandraDriverClient.execute(`CREATE TABLE ${TEST_KEYSPACE}.test(command text PRIMARY KEY, deviceId text)`);
        cassandraStorage.setTableSchemas(tableSchemas).assignTablesToCommands([ 'test' ]);
        
        await cassandraStorage.updateCommand({
            command: 'test command',
            deviceId: 'updated'
        });

        const { rows: updatedRows } = await cassandraDriverClient.execute(`SELECT * FROM ${TEST_KEYSPACE}.test`);
        assert.equal(updatedRows.length, 0);
    });
});

function dropTestKeyspace(cassandraDriverClient) {
    return cassandraDriverClient.execute(`DROP KEYSPACE IF EXISTS ${TEST_KEYSPACE}`);
}

async function createTestKeyspace(cassandraDriverClient) {
    await cassandraDriverClient.execute(`CREATE KEYSPACE ${TEST_KEYSPACE} WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 }`);
    return cassandraDriverClient.execute(`USE ${TEST_KEYSPACE}`);
}