const assert = require('assert');
const sinon = require('sinon');

const TablesBuilder = require('../dataBuilders/TablesBuilder');
const CassandraStorage = require('../../../cassandra/src/CassandraStorage');

describe('Cassandra Storage Provider', () => {
    const MockCassandraClient = class {};
    let dummyCommandData;

    beforeEach(() => {
        MockCassandraClient.prototype.execute = sinon.spy();
        MockCassandraClient.prototype.batch = sinon.spy();

        dummyCommandData = {
            deviceId: 'some-device',
            command: 'command-name',
            timestamp: 1516266743223
        };
    });

    it('Should call execution of UDT creation query for each described UDT', () => {
        const userTypes = {
            test_type1: { prop: 'text' },
            test_type2: { prop: 'text' },
            test_type3: { prop: 'text' }
        };
        const execSpy = MockCassandraClient.prototype.execute;
        const cassandra = new CassandraStorage(new MockCassandraClient());

        cassandra.initializeUDTSchemas(userTypes);

        assert.equal(execSpy.callCount, 3);
    });

    it('Should call execution of table creation query for each described table', () => {
        const tables = new TablesBuilder().withTables('my_table1', 'my_table2', 'my_table3').build();
        const execSpy = MockCassandraClient.prototype.execute;
        const cassandra = new CassandraStorage(new MockCassandraClient());
        
        cassandra.initializeTableSchemas(tables);

        assert.equal(execSpy.callCount, 3);
    });

    it('Should execute insert query only for tables defined as command tables', () => {
        const tables = new TablesBuilder().withTables('commands', 'another_commands').build();
        const batchSpy = MockCassandraClient.prototype.batch;

        const cassandra = new CassandraStorage(new MockCassandraClient());
        cassandra.initializeTableSchemas(tables);
        cassandra.assignTablesToCommands('commands', 'another_commands');

        cassandra.insertCommand(dummyCommandData);

        const batchedQueries = batchSpy.firstCall.args[0];
        assert.equal(batchedQueries.length, 2);
    });

    it('Should execute insert query only for tables defined as notification tables', () => {
        const tables = new TablesBuilder().withTables('commands', 'more_commands', 'notifications', 'shared').build();
        const batchSpy = MockCassandraClient.prototype.batch;

        const cassandra = new CassandraStorage(new MockCassandraClient());
        cassandra.initializeTableSchemas(tables);
        cassandra.assignTablesToNotifications('notifications', 'shared');

        cassandra.insertNotification({
            deviceId: 'some-device',
            notification: 'notif-name',
            timestamp: 1516266743223
        });

        const batchedQueries = batchSpy.firstCall.args[0];
        assert.equal(batchedQueries.length, 2);
    });

    it('Should execute insert query only for tables defined as command updates tables', () => {
        const tables = new TablesBuilder().withTables('commands', 'command_updates', 'notifications', 'shared').build();
        const batchSpy = MockCassandraClient.prototype.batch;

        const cassandra = new CassandraStorage(new MockCassandraClient());
        cassandra.initializeTableSchemas(tables);
        cassandra.assignTablesToCommandUpdates('command_updates', 'shared');

        cassandra.insertCommandUpdate(dummyCommandData);

        const batchedQueries = batchSpy.firstCall.args[0];
        assert.equal(batchedQueries.length, 2);
    });

    it('Should save properties that are defined in JSON table schema and ignore rest', () => {
        const tables = new TablesBuilder().withTables('commands').withSchemaForAll({
            command: 'text',
            timestamp: 'timestamp',
            __primaryKey__: [ 'command' ],
            __clusteredKey__: [ 'timestamp' ]
        }).build();
        const batchSpy = MockCassandraClient.prototype.batch;

        const cassandra = new CassandraStorage(new MockCassandraClient());
        cassandra.initializeTableSchemas(tables);
        cassandra.assignTablesToCommands('commands');

        cassandra.insertCommand(dummyCommandData);

        const batchedQueries = batchSpy.firstCall.args[0];
        assert.equal(batchedQueries.length, 1);
        assert.equal(batchedQueries[0].query, 'INSERT INTO commands (command, timestamp) VALUES (?, ?)');
        assert.deepEqual(batchedQueries[0].params, [ 'command-name', 1516266743223 ]);
    });

    it('Should execute insert query only for tables that defined', () => {
        const tables = new TablesBuilder().withTables('commands').build();
        const batchSpy = MockCassandraClient.prototype.batch;

        const cassandra = new CassandraStorage(new MockCassandraClient());
        cassandra.initializeTableSchemas(tables);
        cassandra.assignTablesToCommands('commands', 'another_command');

        cassandra.insertCommand({});

        const batchedQueries = batchSpy.firstCall.args[0];
        assert.equal(batchedQueries.length, 1);
    });

    it('Should omit columns which are not defined as properties in command object', () => {
        const tables = new TablesBuilder().withTables('commands').withSchemaForAll({
            command: 'text',
            timestamp: 'timestamp',
            someNonexistentField: 'int',
            __primaryKey__: [ 'command' ],
            __clusteredKey__: [ 'timestamp' ]
        }).build();
        const batchSpy = MockCassandraClient.prototype.batch;

        const cassandra = new CassandraStorage(new MockCassandraClient());
        cassandra.initializeTableSchemas(tables);
        cassandra.assignTablesToCommands('commands');

        cassandra.insertCommand(dummyCommandData);

        const batchedQueries = batchSpy.firstCall.args[0];
        assert.equal(batchedQueries[0].query, 'INSERT INTO commands (command, timestamp) VALUES (?, ?)');
        assert.deepEqual(batchedQueries[0].params, [ 'command-name', 1516266743223 ]);
    });

    it('Should insert into columns which are UDT according to their schema', () => {
        const userTypes = {
            parameters: {
                prop1: 'int',
                prop2: 'text'
            }
        };
        const tables = new TablesBuilder().withTables('commands').withSchemaForAll({
            command: 'text',
            params: 'frozen<parameters>',
            __primaryKey__: [ 'command' ]
        }).build();
        const batchSpy = MockCassandraClient.prototype.batch;

        const cassandra = new CassandraStorage(new MockCassandraClient());
        cassandra.initializeUDTSchemas(userTypes);
        cassandra.initializeTableSchemas(tables);
        cassandra.assignTablesToCommands('commands');

        cassandra.insertCommand({
            command: 'command-name',
            params: {
                prop1: 123,
                prop2: 'test value',
                redundant: 'this property must not be included'
            }
        });

        const batchedQueries = batchSpy.firstCall.args[0];
        assert.equal(batchedQueries[0].query, 'INSERT INTO commands (command, params) VALUES (?, ?)');
        assert.deepEqual(batchedQueries[0].params, [ 'command-name', { prop1: 123, prop2: 'test value' } ]);
    });
});