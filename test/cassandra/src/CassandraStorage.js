const assert = require('assert');
const sinon = require('sinon');

const TableMetadataBuilder = require('../dataBuilders/TableMetadataBuilder');
const UDTMetadataBuilder = require('../dataBuilders/UDTMetadataBuilder');
const TablesBuilder = require('../dataBuilders/TablesBuilder');
const CassandraStorage = require('../../../cassandra/src/CassandraStorage');

describe('Cassandra Storage Provider', () => {
    const MockCassandraClient = class {};
    let dummyCommandData;

    beforeEach(() => {
        MockCassandraClient.prototype.execute = sinon.spy();
        MockCassandraClient.prototype.batch = sinon.spy();
        MockCassandraClient.prototype.metadata = {
            getTable: sinon.stub().returns(Promise.resolve({})),
            getUdt: sinon.stub().returns(Promise.resolve({}))
        };
        MockCassandraClient.prototype.keyspace = 'test_keyspace';

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

        cassandra.createUDTSchemas(userTypes);

        assert.equal(execSpy.callCount, 3);
    });

    it('Should call execution of table creation query for each described table', () => {
        const tables = new TablesBuilder().withTables('my_table1', 'my_table2', 'my_table3').build();
        const execSpy = MockCassandraClient.prototype.execute;
        const cassandra = new CassandraStorage(new MockCassandraClient());
        
        cassandra.createTableSchemas(tables);

        assert.equal(execSpy.callCount, 3);
    });

    it('Should execute insert query only for tables defined as command tables', () => {
        const tables = new TablesBuilder().withTables('commands', 'another_commands').build();
        const batchSpy = MockCassandraClient.prototype.batch;

        const cassandra = new CassandraStorage(new MockCassandraClient());
        cassandra.createTableSchemas(tables);
        cassandra.assignTablesToCommands('commands', 'another_commands');

        cassandra.insertCommand(dummyCommandData);

        const batchedQueries = batchSpy.firstCall.args[0];
        assert.equal(batchedQueries.length, 2);
    });

    it('Should execute update query only for tables defined as command tables', () => {
        const tables = new TablesBuilder().withTables('commands', 'another_commands').build();
        const batchSpy = MockCassandraClient.prototype.batch;

        const cassandra = new CassandraStorage(new MockCassandraClient());
        cassandra.createTableSchemas(tables);
        cassandra.assignTablesToCommands('commands', 'another_commands');

        cassandra.updateCommand(dummyCommandData);

        const batchedQueries = batchSpy.firstCall.args[0];
        assert.equal(batchedQueries.length, 2);
    });

    it('Should execute insert query only for tables defined as notification tables', () => {
        const tables = new TablesBuilder().withTables('commands', 'more_commands', 'notifications', 'shared').build();
        const batchSpy = MockCassandraClient.prototype.batch;

        const cassandra = new CassandraStorage(new MockCassandraClient());
        cassandra.createTableSchemas(tables);
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
        cassandra.createTableSchemas(tables);
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
        cassandra.createTableSchemas(tables);
        cassandra.assignTablesToCommands('commands');

        cassandra.insertCommand(dummyCommandData);

        const batchedQueries = batchSpy.firstCall.args[0];
        assert.equal(batchedQueries.length, 1);
        assert.equal(batchedQueries[0].query, 'INSERT INTO test_keyspace.commands (command, timestamp) VALUES (?, ?)');
        assert.deepEqual(batchedQueries[0].params, [ 'command-name', 1516266743223 ]);
    });

    it('Should execute insert query only for tables that defined', () => {
        const tables = new TablesBuilder().withTables('commands').build();
        const batchSpy = MockCassandraClient.prototype.batch;

        const cassandra = new CassandraStorage(new MockCassandraClient());
        cassandra.createTableSchemas(tables);
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
        cassandra.createTableSchemas(tables);
        cassandra.assignTablesToCommands('commands');

        cassandra.insertCommand(dummyCommandData);

        const batchedQueries = batchSpy.firstCall.args[0];
        assert.equal(batchedQueries[0].query, 'INSERT INTO test_keyspace.commands (command, timestamp) VALUES (?, ?)');
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
        cassandra.createUDTSchemas(userTypes);
        cassandra.createTableSchemas(tables);
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
        assert.equal(batchedQueries[0].query, 'INSERT INTO test_keyspace.commands (command, params) VALUES (?, ?)');
        assert.deepEqual(batchedQueries[0].params, [ 'command-name', { prop1: 123, prop2: 'test value' } ]);
    });

    it('Should check tables and UDT existence', () => {
        const { getTable, getUdt } = MockCassandraClient.prototype.metadata;
        const cassandra = new CassandraStorage(new MockCassandraClient());
        cassandra.setTableSchemas({
            table: {}
        }).setUDTSchemas({
            udt: {}
        });

        cassandra.checkSchemasExistence(() => {});

        assert.ok(getTable.calledOnce);
        assert.deepEqual(getTable.firstCall.args, [ 'test_keyspace', 'table' ]);
        assert.ok(getUdt.calledOnce);
        assert.deepEqual(getUdt.firstCall.args, [ 'test_keyspace', 'udt' ]);
    });

    it('Should invoke callback with false if some schemas do not exist', done => {
        const { getUdt } = MockCassandraClient.prototype.metadata;
        getUdt.returns(Promise.resolve(null));

        const cassandra = new CassandraStorage(new MockCassandraClient());
        cassandra.setTableSchemas({
            table: {}
        }).setUDTSchemas({
            udt: {}
        });

        const callback = sinon.stub();
        cassandra.checkSchemasExistence(callback);

        asyncAssertion(() => {
            assert.ok(callback.calledOnce);
            assert.equal(callback.firstCall.args[0], false);
            done();
        });
    });

    it('Should emit "tableExists" event if table already exists', done => {
        const metadata = new TableMetadataBuilder().withName('testTable').withIntColumn('col1').build();
        MockCassandraClient.prototype.metadata.getTable.returns(Promise.resolve(metadata));
        const cassandra = new CassandraStorage(new MockCassandraClient());
        const schemas = {
            testTable: {
                col1: 'int',
                __primaryKey__: [ 'col1' ]
            }
        };

        cassandra.setTableSchemas(schemas);
        const eventEmitter = cassandra.compareTableSchemas();

        sinon.spy(eventEmitter, 'emit');

        asyncAssertion(() => {
            assert.equal(eventEmitter.emit.callCount, 2, 'Number of times event was emitted');

            const [ event, tableName ] = eventEmitter.emit.firstCall.args;
            assert.equal(event, 'tableExists');
            assert.equal(tableName, 'testTable');
            done();
        });
    });

    it('Should emit "done" event when table comparison is done', done => {
        MockCassandraClient.prototype.metadata.getTable.returns(Promise.resolve(null));
        const cassandra = new CassandraStorage(new MockCassandraClient());
        const schemas = {
            testTable: {
                col1: 'int',
                __primaryKey__: [ 'col1' ]
            }
        };

        cassandra.setTableSchemas(schemas);
        const eventEmitter = cassandra.compareTableSchemas();

        sinon.spy(eventEmitter, 'emit');

        asyncAssertion(() => {
            assert.equal(eventEmitter.emit.callCount, 1, 'Number of times event was emitted');

            const [ event ] = eventEmitter.emit.firstCall.args;
            assert.equal(event, 'done');

            done();
        });
    });

    it('Should emit "columnTypesMismatch" event if table contains same columns as schema but different types', done => {
        const metadata = new TableMetadataBuilder().withName('testTable').withIntColumn('col1').withTextColumn('col2').build();
        MockCassandraClient.prototype.metadata.getTable.returns(Promise.resolve(metadata));
        const cassandra = new CassandraStorage(new MockCassandraClient());
        const schemas = {
            testTable: {
                col1: 'int',
                col2: 'int',
                __primaryKey__: [ 'col1' ],
                __clusteredKey__: [ 'col2' ]
            }
        };

        cassandra.setTableSchemas(schemas);
        const eventEmitter = cassandra.compareTableSchemas();

        sinon.spy(eventEmitter, 'emit');

        asyncAssertion(() => {
            assert.equal(eventEmitter.emit.callCount, 3, 'Number of times event was emitted');

            const [ event, tableName, propName, realType, schemaType ] = eventEmitter.emit.secondCall.args;
            assert.equal(event, 'columnTypesMismatch');
            assert.equal(tableName, 'testTable');
            assert.equal(propName, 'col2');
            assert.equal(realType, 'text');
            assert.equal(schemaType, 'int');

            done();
        });
    });

    it('Should emit "columnsMismatch" event if schema has columns which real table does not have', done => {
        const metadata = new TableMetadataBuilder().withName('testTable').withIntColumn('col1').withTextColumn('col2').build();
        MockCassandraClient.prototype.metadata.getTable.returns(Promise.resolve(metadata));
        const cassandra = new CassandraStorage(new MockCassandraClient());
        const schemas = {
            testTable: {
                col1: 'int',
                col3: 'int',
                __primaryKey__: [ 'col1' ],
                __clusteredKey__: [ 'col3' ]
            }
        };

        cassandra.setTableSchemas(schemas);
        const eventEmitter = cassandra.compareTableSchemas();

        sinon.spy(eventEmitter, 'emit');

        asyncAssertion(() => {
            assert.equal(eventEmitter.emit.callCount, 3, 'Number of times event was emitted');

            const [ event, tableName ] = eventEmitter.emit.secondCall.args;
            assert.equal(event, 'columnsMismatch');
            assert.equal(tableName, 'testTable');

            done();
        });
    });

    it('Should emit "customTypeExists" event if user defined type already exists', done => {
        const metadata = new UDTMetadataBuilder().withName('test_udt').withIntField('test_field').build();
        MockCassandraClient.prototype.metadata.getUdt.returns(Promise.resolve(metadata));
        const cassandra = new CassandraStorage(new MockCassandraClient());
        const schemas = {
            test_udt: {
                test_field: 'int'
            }
        };

        cassandra.setUDTSchemas(schemas);
        const eventEmitter = cassandra.compareUDTSchemas();

        sinon.spy(eventEmitter, 'emit');

        asyncAssertion(() => {
            assert.equal(eventEmitter.emit.callCount, 2, 'Number of times event was emitted');

            const [ event, udtName ] = eventEmitter.emit.firstCall.args;
            assert.equal(event, 'customTypeExists');
            assert.equal(udtName, 'test_udt');
            done();
        });
    });

    it('Should emit "done" event when UDT comparison is done', done => {
        MockCassandraClient.prototype.metadata.getUdt.returns(Promise.resolve(null));
        const cassandra = new CassandraStorage(new MockCassandraClient());
        const schemas = {
            test_udt: {
                test_field: 'int'
            }
        };

        cassandra.setUDTSchemas(schemas);
        const eventEmitter = cassandra.compareUDTSchemas();

        sinon.spy(eventEmitter, 'emit');

        asyncAssertion(() => {
            assert.equal(eventEmitter.emit.callCount, 1, 'Number of times event was emitted');

            const [ event ] = eventEmitter.emit.firstCall.args;
            assert.equal(event, 'done');

            done();
        });
    });

    it('Should emit "fieldTypesMismatch" event if UDT contains same columns as schema but different types', done => {
        const metadata = new UDTMetadataBuilder().withName('test_udt').withIntField('field1').withTextField('field2').build();
        MockCassandraClient.prototype.metadata.getUdt.returns(Promise.resolve(metadata));
        const cassandra = new CassandraStorage(new MockCassandraClient());
        const schemas = {
            test_udt: {
                field1: 'int',
                field2: 'int'
            }
        };

        cassandra.setUDTSchemas(schemas);
        const eventEmitter = cassandra.compareUDTSchemas();

        sinon.spy(eventEmitter, 'emit');

        asyncAssertion(() => {
            assert.equal(eventEmitter.emit.callCount, 3, 'Number of times event was emitted');

            const [ event, udtName, fieldName, realType, schemaType ] = eventEmitter.emit.secondCall.args;
            assert.equal(event, 'fieldTypesMismatch');
            assert.equal(udtName, 'test_udt');
            assert.equal(fieldName, 'field2');
            assert.equal(realType, 'text');
            assert.equal(schemaType, 'int');

            done();
        });
    });

    it('Should emit "fieldsMismatch" event if schema has columns which real UDT does not have', done => {
        const metadata = new UDTMetadataBuilder().withName('test_udt').withIntField('field1').withIntField('field2').build();
        MockCassandraClient.prototype.metadata.getUdt.returns(Promise.resolve(metadata));
        const cassandra = new CassandraStorage(new MockCassandraClient());
        const schemas = {
            test_udt: {
                field1: 'int',
                field3: 'int'
            }
        };

        cassandra.setUDTSchemas(schemas);
        const eventEmitter = cassandra.compareUDTSchemas();

        sinon.spy(eventEmitter, 'emit');

        asyncAssertion(() => {
            assert.equal(eventEmitter.emit.callCount, 3, 'Number of times event was emitted');

            const [ event, udtName ] = eventEmitter.emit.secondCall.args;
            assert.equal(event, 'fieldsMismatch');
            assert.equal(udtName, 'test_udt');

            done();
        });
    });

    it('Should drop tables which defined in schema with __dropIfExists__ as true', () => {
        const mockCassandraClient = new MockCassandraClient();
        const execSpy = mockCassandraClient.execute;
        const cassandra = new CassandraStorage(mockCassandraClient);
        const schemas = {
            test: {
                id: 'int',
                __primaryKey__: [ 'id' ]
            },

            dropThis: {
                id: 'int',
                __primaryKey__: [ 'id' ],
                __dropIfExists__: true
            },

            shouldBeDropped: {
                id: 'int',
                __primaryKey__: [ 'id' ],
                __dropIfExists__: true
            }
        };

        cassandra.dropTableSchemas(schemas);

        assert.equal(execSpy.callCount, 2);
    });

    it('Should drop types which defined in schema with __dropIfExists__ as true', () => {
        const mockCassandraClient = new MockCassandraClient();
        const execSpy = mockCassandraClient.execute;
        const cassandra = new CassandraStorage(mockCassandraClient);
        const schemas = {
            test: {
                id: 'int'
            },

            dropThis: {
                id: 'int',
                __dropIfExists__: true
            },

            shouldBeDropped: {
                id: 'int',
                __dropIfExists__: true
            }
        };

        cassandra.dropTypeSchemas(schemas);

        assert.equal(execSpy.callCount, 2);
    });
});

function asyncAssertion(callback) {
    setTimeout(callback, 0);
}