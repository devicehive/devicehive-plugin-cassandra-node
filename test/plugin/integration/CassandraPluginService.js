const assert = require('assert');
const sinon = require('sinon');
const cassandraDriver = require('cassandra-driver');

const CassandraPluginService = require('../../../plugin/CassandraPluginService');
const cassandraSetup = require('./cassandraSetup');

const TEST_KEYSPACE = 'plugin_integration_tests';
const CASSANDRA_CONFIG = {
    CONNECTION: {
        CONTACT_POINTS: '127.0.0.1',
        KEYSPACE: TEST_KEYSPACE
    },

    custom: {
        schemaChecksCount: 10,
        schemaChecksInterval: 250
    }
};
const CASSANDRA_DRIVER_CONFIG = {
    contactPoints: [ '127.0.0.1' ]
};

const testCassandraTables = {
    tables: {
        test_commands: {
            command: 'text',
            deviceId: 'text',
            __primaryKey__: [ 'command' ]
        },

        test_notifications: {
            notification: 'text',
            deviceId: 'text',
            __primaryKey__: [ 'notification' ]
        },

        test_command_updates: {
            command: 'text',
            deviceId: 'text',
            __primaryKey__: [ 'command' ]
        }
    },

    commandTables: [ 'test_commands' ],
    commandUpdatesTables: [ 'test_command_updates' ],
    notificationTables: [ 'test_notifications' ]
};

describe('Plugin integration tests', function() {
    this.timeout(5000);

    const cassandraDriverClient = new cassandraDriver.Client(CASSANDRA_DRIVER_CONFIG);
    let plugin;

    before(async () => {
        await cassandraDriverClient.connect();
    });

    beforeEach(async () => {
        await cassandraSetup.dropTestKeyspace(cassandraDriverClient, TEST_KEYSPACE);
        await cassandraSetup.createTestKeyspace(cassandraDriverClient, TEST_KEYSPACE);
        await createTables(cassandraDriverClient, testCassandraTables);

        plugin = new CassandraPluginService(CASSANDRA_CONFIG);
        plugin.cassandra = await plugin.initCassandra(CASSANDRA_CONFIG, { udts: {}, tables: testCassandraTables });
    });

    after(async () => {
        await cassandraSetup.dropTestKeyspace(cassandraDriverClient, TEST_KEYSPACE);
    });

    it('Should insert command', done => {
        plugin.handleCommand({
            deviceId: 'test123',
            command: 'command name'
        });

        asyncAssertion(async () => {
            const { rows: commands } = await cassandraDriverClient.execute(`SELECT * FROM test_commands`);
            assert.equal(commands.length, 1);
            done();
        });
    });

    it('Should insert notification', done => {
        plugin.handleNotification({
            deviceId: 'test123',
            notification: 'notification name'
        });

        asyncAssertion(async () => {
            const { rows: notifications } = await cassandraDriverClient.execute(`SELECT * FROM test_notifications`);
            assert.equal(notifications.length, 1);
            done();
        });
    });

    it('Should insert command update in case command updates storing is enabled', done => {
        sinon.stub(plugin, 'isCommandUpdatesStoringEnabled').returns(true);

        plugin.handleCommandUpdate({
            deviceId: 'test123',
            command: 'command name'
        });

        plugin.isCommandUpdatesStoringEnabled.restore();
        asyncAssertion(async () => {
            const { rows: commandUpdates } = await cassandraDriverClient.execute(`SELECT * FROM test_command_updates`);
            assert.equal(commandUpdates.length, 1);
            done();
        });
    });

    it('Should update command in case command updates storing is disabled', done => {
        sinon.stub(plugin, 'isCommandUpdatesStoringEnabled').returns(false);

        const insert = `INSERT INTO test_commands(command, deviceid) VALUES('command name', 'test')`;
        cassandraDriverClient.execute(insert).then(() => {
            plugin.handleCommandUpdate({
                deviceId: 'updated',
                command: 'command name'
            });

            plugin.isCommandUpdatesStoringEnabled.restore();
            asyncAssertion(async () => {
                const { rows: commands } = await cassandraDriverClient.execute(`SELECT * FROM test_commands`);
                assert.equal(commands.length, 1);
                assert.equal(commands[0].deviceid, 'updated');
                done();
            });
        });
    });
});

function asyncAssertion(callback) {
    setTimeout(callback, 500);
}

async function createTables(cassandraDriverClient) {
    await cassandraDriverClient.execute('CREATE TABLE test_commands(command text, deviceId text, PRIMARY KEY((command)))');
    await cassandraDriverClient.execute('CREATE TABLE test_notifications(notification text, deviceId text, PRIMARY KEY((notification)))');
    return cassandraDriverClient.execute('CREATE TABLE test_command_updates(command text, deviceId text, PRIMARY KEY((command)))');
}