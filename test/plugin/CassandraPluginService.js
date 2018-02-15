const assert = require('assert');
const sinon = require('sinon');

const MessageBuilder = require('./dataBuilders/MessageBuilder');

const cassandraConfig = require('../../plugin/config').cassandra;
const cassandraStorage = require('../../cassandra');
const CassandraPluginService = require('../../plugin/CassandraPluginService');

describe('Plugin', () => {
    let cassandra;
    beforeEach(() => {
        cassandra = {};

        cassandra.setTableSchemas = sinon.stub().returns(cassandra);
        cassandra.setUDTSchemas = sinon.stub().returns(cassandra);
        cassandra.assignTablesToCommands = sinon.stub().returns(cassandra);
        cassandra.assignTablesToNotifications = sinon.stub().returns(cassandra);
        cassandra.assignTablesToCommandUpdates = sinon.stub().returns(cassandra);
        cassandra.insertCommand = sinon.stub().returns(Promise.resolve({}));
        cassandra.updateCommand = sinon.stub().returns(Promise.resolve({}));
        cassandra.insertCommandUpdate = sinon.stub().returns(Promise.resolve({}));
        cassandra.insertNotification = sinon.stub().returns(Promise.resolve({}));
        cassandra.checkSchemasExistence = sinon.stub().returns(cassandra).callsFake(cb => cb(true));

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

        sinon.stub(cassandraStorage, 'connect').returns(Promise.resolve(cassandra));

        cassandraConfig.CUSTOM.SCHEMA_CHECKS_COUNT = 10;
        cassandraConfig.CUSTOM.SCHEMA_CHECKS_INTERVAL = 0;
    });

    afterEach(() => {
        cassandraStorage.connect.restore();
    });

    it('Should fail application if Cassandra initialization failed', done => {
        const plugin = new CassandraPluginService();

        sinon.stub(process, 'exit');
        sinon.stub(plugin, 'initCassandra').returns(Promise.reject('error'));

        plugin.afterStart();

        asyncAssertion(() => {
            assert(process.exit.calledOnce);

            plugin.initCassandra.restore();
            process.exit.restore();
            done();
        });
    });

    it('Should set schemas of Cassandra user defined types and tables after start', done => {
        const plugin = new CassandraPluginService();

        plugin.afterStart();

        asyncAssertion(() => {
            assert(cassandra.setTableSchemas.calledOnce);
            assert(cassandra.setUDTSchemas.calledOnce);
            done();
        });
    });

    it('Should assign tables to each group after start', done => {
        const plugin = new CassandraPluginService();

        plugin.afterStart();

        asyncAssertion(() => {
            assert(cassandra.assignTablesToCommands.calledOnce);
            assert(cassandra.assignTablesToNotifications.calledOnce);
            assert(cassandra.assignTablesToCommandUpdates.calledOnce);
            done();
        });
    });

    it('Should insert command if message is command type', done => {
        const msg = new MessageBuilder().withCommand({
            deviceId: 'test123',
            command: 'command name'
        }).build();

        const plugin = new CassandraPluginService();
        sinon.stub(plugin, 'initCassandra').returns(Promise.resolve(cassandra));

        plugin.afterStart();

        asyncAssertion(() => {
            plugin.handleMessage(msg);

            plugin.initCassandra.restore();

            assert(cassandra.insertCommand.calledOnce);
            done();
        });
    });

    it('Should insert command update if message is command type with isUpdated equals true and command updates storing is enabled', done => {
        const msg = new MessageBuilder().withCommand({
            deviceId: 'test123',
            command: 'command name',
            isUpdated: true
        }).build();

        const conf = require('../../plugin/config').cassandra;
        const commandsUpdatesStoring = conf.CUSTOM.COMMAND_UPDATES_STORING;
        conf.CUSTOM.COMMAND_UPDATES_STORING = true;

        const plugin = new CassandraPluginService();
        sinon.stub(plugin, 'initCassandra').returns(Promise.resolve(cassandra));

        plugin.afterStart();

        asyncAssertion(() => {
            plugin.handleMessage(msg);

            plugin.initCassandra.restore();
            conf.CUSTOM.COMMAND_UPDATES_STORING = commandsUpdatesStoring;

            assert(cassandra.insertCommandUpdate.calledOnce);

            done();
        });
    });

    it('Should update commands if command updates storing is disabled', done => {
        const msg = new MessageBuilder().withCommand({
            deviceId: 'test123',
            command: 'command name',
            isUpdated: true
        }).build();

        const conf = require('../../plugin/config').cassandra;
        const commandsUpdatesStoring = conf.CUSTOM.COMMAND_UPDATES_STORING;
        conf.CUSTOM.COMMAND_UPDATES_STORING = false;

        const plugin = new CassandraPluginService();
        sinon.stub(plugin, 'initCassandra').returns(Promise.resolve(cassandra));

        plugin.afterStart();

        asyncAssertion(() => {
            plugin.handleMessage(msg);

            plugin.initCassandra.restore();
            conf.CUSTOM.COMMAND_UPDATES_STORING = commandsUpdatesStoring;

            assert(cassandra.updateCommand.calledOnce);

            done();
        });
    });

    it('Should insert notification if message is notification type', done => {
        const msg = new MessageBuilder().withNotification({
            deviceId: 'test123',
            notification: 'notification name'
        }).build();

        const plugin = new CassandraPluginService();
        sinon.stub(plugin, 'initCassandra').returns(Promise.resolve(cassandra));

        plugin.afterStart();

        asyncAssertion(() => {
            plugin.handleMessage(msg);

            plugin.initCassandra.restore();

            assert(cassandra.insertNotification.calledOnce);
            done();
        });
    });
});

function asyncAssertion(callback) {
    setTimeout(callback, 100);
}