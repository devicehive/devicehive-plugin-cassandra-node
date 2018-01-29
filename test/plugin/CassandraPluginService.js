const assert = require('assert');
const sinon = require('sinon');

const MessageBuilder = require('./dataBuilders/MessageBuilder');

const cassandraStorage = require('../../cassandra');
const CassandraPluginService = require('../../plugin/CassandraPluginService');

describe('Plugin', () => {
    let cassandra;
    beforeEach(() => {
        cassandra = {
            initializeTableSchemas: sinon.stub().returns(Promise.resolve({})),
            initializeUDTSchemas: sinon.stub().returns(Promise.resolve({})),
            assignTablesToCommands: sinon.stub().returns(cassandra),
            assignTablesToNotifications: sinon.stub().returns(cassandra),
            assignTablesToCommandUpdates: sinon.stub().returns(cassandra),
            insertCommand: sinon.stub().returns(cassandra),
            insertCommandUpdate: sinon.stub().returns(cassandra),
            insertNotification: sinon.stub().returns(cassandra)
        };

        cassandraStorage.connect = sinon.stub().returns(cassandra);
    });

    it('Should initialize Cassandra user defined types and tables after start', done => {
        const plugin = new CassandraPluginService();

        plugin.afterStart();

        asyncAssertion(() => {
            assert(cassandra.initializeUDTSchemas.calledOnce);
            assert(cassandra.initializeTableSchemas.calledOnce);
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
        plugin.afterStart();

        asyncAssertion(() => {
            plugin.handleMessage(msg);

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
        plugin.afterStart();

        asyncAssertion(() => {
            plugin.handleMessage(msg);

            assert(cassandra.insertCommandUpdate.calledOnce);

            conf.CUSTOM.COMMAND_UPDATES_STORING = commandsUpdatesStoring;
            done();
        });
    });

    it('Should NOT insert command update if command updates storing is disabled', done => {
        const msg = new MessageBuilder().withCommand({
            deviceId: 'test123',
            command: 'command name',
            isUpdated: true
        }).build();

        const conf = require('../../plugin/config').cassandra;
        const commandsUpdatesStoring = conf.CUSTOM.COMMAND_UPDATES_STORING;
        conf.CUSTOM.COMMAND_UPDATES_STORING = false;

        const plugin = new CassandraPluginService();
        plugin.afterStart();

        asyncAssertion(() => {
            plugin.handleMessage(msg);

            assert(cassandra.insertCommandUpdate.notCalled);

            conf.CUSTOM.COMMAND_UPDATES_STORING = commandsUpdatesStoring;
            done();
        });
    });

    it('Should insert notification if message is notification type', done => {
        const msg = new MessageBuilder().withNotification({
            deviceId: 'test123',
            notification: 'notification name'
        }).build();

        const plugin = new CassandraPluginService();
        plugin.afterStart();

        asyncAssertion(() => {
            plugin.handleMessage(msg);

            assert(cassandra.insertNotification.calledOnce);
            done();
        });
    });
});

function asyncAssertion(callback) {
    setTimeout(callback, 0);
}