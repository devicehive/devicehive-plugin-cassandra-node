const assert = require('assert');
const sinon = require('sinon');

const cassandraStorage = require('../../cassandra/index');

describe('Cassandra schemas creation', () => {
    it('Should create Cassandra user defined types and tables on script run', done => {
        const cassandra = {
            createTableSchemas: sinon.stub().returns(Promise.resolve({})),
            createUDTSchemas: sinon.stub().returns(Promise.resolve({}))
        };

        cassandraStorage.connect = sinon.stub().returns(Promise.resolve(cassandra));

        const exit = process.exit;
        process.exit = sinon.spy();

        require('../../plugin/createCassandraSchemas');

        asyncAssertion(() => {
            assert(cassandra.createUDTSchemas.calledOnce);
            assert(cassandra.createTableSchemas.calledOnce);

            process.exit = exit;
            done();
        });
    });
});

function asyncAssertion(callback) {
    setTimeout(callback, 0);
}