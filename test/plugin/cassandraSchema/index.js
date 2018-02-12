const assert = require('assert');
const sinon = require('sinon');

const cassandraStorage = require('../../../cassandra');

describe('Cassandra schemas creation', () => {
    const sandbox = sinon.createSandbox();
    let cassandra;


    beforeEach(() => {
        cassandra = {};

        cassandra.createTableSchemas = sinon.stub().returns(Promise.resolve({}));
        cassandra.createUDTSchemas = sinon.stub().returns(Promise.resolve({}));
        cassandra.setTableSchemas = sinon.stub().returns(cassandra);
        cassandra.setUDTSchemas = sinon.stub().returns(cassandra);

        const notifier = {
            on(event, handler) {
                if (event === 'done') {
                    handler();
                }

                return this;
            }
        };
        cassandra.compareTableSchemas = sinon.stub().returns(notifier);
        cassandra.compareUDTSchemas = sinon.stub().returns(notifier);

        sandbox.stub(cassandraStorage, 'connect').returns(Promise.resolve(cassandra));
        sandbox.stub(process, 'exit');
    });

    afterEach(() => {
        delete require.cache[require.resolve('../../../plugin/cassandraSchema')];
        sandbox.restore();
    });

    it('Should create Cassandra user defined types and tables on script run', done => {
        require('../../../plugin/cassandraSchema');

        asyncAssertion(() => {
            assert(cassandra.createUDTSchemas.calledOnce);
            assert(cassandra.createTableSchemas.calledOnce);

            done();
        });
    });

    it('Should fail Cassandra schema creation if "parameters" field is not basic type', () => {
        require('../../../cassandraSchemas/cassandra-tables').tables = {
            test: {
                id: 'int',
                parameters: 'frozen<list<int>>',
                __primaryKey__: [ 'id' ]
            }
        };

        require('../../../plugin/cassandraSchema');

        assert(process.exit.calledWith(1));
    });
});

function asyncAssertion(callback) {
    setTimeout(callback, 0);
}