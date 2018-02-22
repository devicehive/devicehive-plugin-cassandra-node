const assert = require('assert');
const cassandraDriver = require('cassandra-driver');

const cassandraInit = require('../../../plugin/cassandraInit');
const cassandraSetup = require('./cassandraSetup');

const TEST_KEYSPACE = 'plugin_integration_tests';
const CASSANDRA_CONFIG = {
    connection: {
        contactPoints: '127.0.0.1',
        keyspace: TEST_KEYSPACE
    },

    custom: {
        schemaChecksCount: 10,
        schemaChecksInterval: 250
    }
};
const CASSANDRA_DRIVER_CONFIG = {
    contactPoints: [ '127.0.0.1' ]
};

const cassandraTestTables = {
    tables: {
        commands: {
            command: 'text',
            __primaryKey__: [ 'command' ]
        },

        notifications: {
            notification: 'text',
            __primaryKey__: [ 'notification' ]
        },

        command_updates: {
            command: 'text',
            __primaryKey__: [ 'command' ]
        }
    },

    commandTables: [ 'commands' ],
    commandUpdatesTables: [ 'command_updates' ],
    notificationTables: [ 'notifications' ]
};

describe('Cassandra Storage in plugin initialization integration tests', function() {
    this.timeout(5000);

    const cassandraDriverClient = new cassandraDriver.Client(CASSANDRA_DRIVER_CONFIG);

    before(async () => {
        await cassandraDriverClient.connect();
    });

    beforeEach(async () => {
        await cassandraSetup.dropTestKeyspace(cassandraDriverClient, TEST_KEYSPACE);
        await cassandraSetup.createTestKeyspace(cassandraDriverClient, TEST_KEYSPACE);
    });

    after(async () => {
        await cassandraSetup.dropTestKeyspace(cassandraDriverClient, TEST_KEYSPACE);
    });

    it('Should init Cassandra Storage client only after schemas have been created', async () => {
        cassandraDriverClient.execute('CREATE TABLE commands(command text PRIMARY KEY)');
        cassandraDriverClient.execute('CREATE TABLE notifications(notification text PRIMARY KEY)');
        cassandraDriverClient.execute('CREATE TABLE command_updates(command text PRIMARY KEY)');

        const cassandra = await cassandraInit(CASSANDRA_CONFIG, { udts: {}, tables: cassandraTestTables });
        assert.notEqual(cassandra, null);
    });
});