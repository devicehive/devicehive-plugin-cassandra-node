const assert = require('assert');
const cassandraDriver = require('cassandra-driver');

const MessageBuilder = require('../dataBuilders/MessageBuilder');
const CassandraPluginService = require('../../../plugin/CassandraPluginService');
const cassandraTables = require('../../../cassandraSchemas/cassandra-tables');
const testCassandraTables = require('./cassandra-tables');

const TEST_KEYSPACE = 'plugin_integration_tests';
const CASSANDRA_CONFIG = {
    CONNECTION: {
        CONTACT_POINTS: '127.0.0.1',
        KEYSPACE: TEST_KEYSPACE
    },

    CUSTOM: {
        SCHEMA_CHECKS_COUNT: 10,
        SCHEMA_CHECKS_INTERVAL: 10000
    }
};
const CASSANDRA_DRIVER_CONFIG = {
    contactPoints: [ '127.0.0.1' ]
};

cassandraTables.tables = testCassandraTables.tables;
cassandraTables.commandTables = testCassandraTables.commandTables;
cassandraTables.commandUpdatesTables = testCassandraTables.commandUpdatesTables;
cassandraTables.notificationTables = testCassandraTables.notificationTables;

describe('Plugin integration tests', function() {
    this.timeout(5000);

    const cassandraDriverClient = new cassandraDriver.Client(CASSANDRA_DRIVER_CONFIG);

    before(async () => {
        await cassandraDriverClient.connect();
    });

    beforeEach(async () => {
        await dropTestKeyspace(cassandraDriverClient);
        await createTestKeyspace(cassandraDriverClient);
        await createTables(cassandraDriverClient, testCassandraTables);
    });

    after(async () => {
        await dropTestKeyspace(cassandraDriverClient);
    });

    it('Should insert command', done => {
        const msg = new MessageBuilder().withCommand({
            deviceId: 'test123',
            command: 'command name'
        }).build();
        const plugin = new CassandraPluginService(CASSANDRA_CONFIG);
        plugin.afterStart();

        setTimeout(() => {
            plugin.handleCommand(msg);
        }, 500);

        asyncAssertion(async () => {
            const { rows: commands } = await cassandraDriverClient.execute(`SELECT * FROM ${testCassandraTables.testCommandsTableName}`);
            assert.equal(commands.length, 1);
            done();
        });
    });
});

function asyncAssertion(callback) {
    setTimeout(callback, 1000);
}

function dropTestKeyspace(cassandraDriverClient) {
    return cassandraDriverClient.execute(`DROP KEYSPACE IF EXISTS ${TEST_KEYSPACE}`);
}

async function createTestKeyspace(cassandraDriverClient) {
    await cassandraDriverClient.execute(`CREATE KEYSPACE ${TEST_KEYSPACE} WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 }`);
    return cassandraDriverClient.execute(`USE ${TEST_KEYSPACE}`);
}

async function createTables(cassandraDriverClient, testCassandraTables) {
    await cassandraDriverClient.execute(testCassandraTables.testCommandsTableCreate);
    await cassandraDriverClient.execute(testCassandraTables.testNotificationsTableCreate);
    return cassandraDriverClient.execute(testCassandraTables.testCommandUpdatesTableCreate);
}