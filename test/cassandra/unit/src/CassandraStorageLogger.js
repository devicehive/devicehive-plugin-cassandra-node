const assert = require('assert');
const sinon = require('sinon');

const CassandraStorageLogger = require('../../../../cassandra/src/CassandraStorageLogger');

describe('Cassandra Storage Logger', () => {
    let cassandraStorageSpy;
    const log = sinon.spy();

    beforeEach(() => {
        cassandraStorageSpy = {
            _execute(query) {
                return Promise.resolve({});
            },
            _batchExecute(queries) {
                return Promise.resolve({});
            },
            createTableSchemas(tables) {},
            dropTableSchemas(tables) {},
            createUDTSchemas(types) {},
            dropTypeSchemas(types) {},
            checkSchemasExistence(callback) {},
            compareTableSchemas() {},
            _queryTableGroup(groupName, queryBuilder) {}
        };

        const storageLogger = new CassandraStorageLogger(log);
        storageLogger.attach(cassandraStorageSpy);
    });

    afterEach(() => {
        log.resetHistory();
    });

    it('Should log query to be executed', () => {
        const query = 'Test query';

        cassandraStorageSpy._execute(query);

        assert.equal(log.firstCall.args[0], 'Executing Test query');
    });

    it('Should log successful query execution', done => {
        const query = 'Test query';

        cassandraStorageSpy._execute(query);

        asyncAssertion(() => {
            assert(log.calledTwice);
            assert.equal(log.secondCall.args[0], 'Executed successfully: Test query');
            done();
        });
    });

    it('Should log NOT successful query execution', done => {
        const query = 'Test query';
        const storageLogger = new CassandraStorageLogger(log);
        const cassandraStorageSpy = {
            _execute(query) {
                return Promise.reject({ message: 'test error' });
            }
        };

        storageLogger.attach(cassandraStorageSpy);

        cassandraStorageSpy._execute(query);

        asyncAssertion(() => {
            assert(log.calledTwice);
            assert.equal(log.secondCall.args[0], 'Query failed: Test query; Error: test error');
            done();
        });
    });

    it('Should log batched execution', () => {
        const query1 = 'Test query 1';
        const query2 = 'Test query 2';

        cassandraStorageSpy._batchExecute([ query1, query2 ]);

        assert.equal(log.firstCall.args[0], 'Executing batch ["Test query 1","Test query 2"]');
    });

    it('Should log successful batched execution', done => {
        const query1 = 'Test query 1';
        const query2 = 'Test query 2';

        cassandraStorageSpy._batchExecute([ query1, query2 ]);

        asyncAssertion(() => {
            assert(log.calledTwice);
            assert.equal(log.secondCall.args[0], 'Batched queries executed successfully ["Test query 1","Test query 2"]');
            done();
        });
    });

    it('Should log NOT successful batched execution', done => {
        const query1 = 'Test query 1';
        const query2 = 'Test query 2';
        const storageLogger = new CassandraStorageLogger(log);
        const cassandraStorageSpy = {
            _batchExecute(queries) {
                return Promise.reject({ message: 'test error' });
            }
        };

        storageLogger.attach(cassandraStorageSpy);

        cassandraStorageSpy._batchExecute([ query1, query2 ]);

        asyncAssertion(() => {
            assert(log.calledTwice);
            assert.equal(log.secondCall.args[0], 'Batched queries failed ["Test query 1","Test query 2"]; Error: test error');
            done();
        });
    });

    it('Should log table schemas creation', () => {
        cassandraStorageSpy.createTableSchemas({
            table1: {},
            table2: {}
        });

        assert.equal(log.firstCall.args[0], 'Creating 2 tables');
    });

    it('Should log UDT schemas creation', () => {
        cassandraStorageSpy.createUDTSchemas({
            type1: {},
            type2: {}
        });

        assert.equal(log.firstCall.args[0], 'Creating 2 user defined types');
    });

    it('Should log drop of table schemas', () => {
        cassandraStorageSpy.dropTableSchemas({
            table1: {},
            table2: {}
        });

        assert(log.firstCall.args[0], 'Will try to drop 2 table schemas');
    });

    it('Should log drop of UDT schemas', () => {
        cassandraStorageSpy.dropTypeSchemas({
            type1: {},
            type2: {}
        });

        assert(log.firstCall.args[0], 'Will try to drop 2 user defined type schemas');
    });

    it('Should log schema existence check', () => {
        cassandraStorageSpy.checkSchemasExistence(() => {});
        assert(log.firstCall.args[0], 'Checking schema existence');
    });

    it('Should log table schema comparison', () => {
        cassandraStorageSpy.compareTableSchemas(() => {});
        assert(log.firstCall.args[0], 'Comparing table schemas');
    });

    it('Should log query to table group', () => {
        cassandraStorageSpy._queryTableGroup('commands');
        assert(log.firstCall.args[0], 'Querying table group: commands');
    });
});

function asyncAssertion(assertion) {
    setTimeout(assertion, 0);
}