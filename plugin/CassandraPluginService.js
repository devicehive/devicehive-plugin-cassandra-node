const { DeviceHivePlugin } = require(`devicehive-plugin-core`);
const { MessageBuilder, MessageUtils } = require(`devicehive-proxy-message`);

const cassandraConfig = require(`./config`).cassandra;
const cassandraTables = require('./cassandra-tables');
const cassandraUDTs = require('./cassandra-user-types');
const CassandraStorage = require('../cassandra');

/**
 * Plugin main class
 */
class CassandraPluginService extends DeviceHivePlugin {

    /**
     * Before plugin starts hook
     */
    beforeStart() {
        console.log(`Plugin is starting`);
    }

    /**
     * After plugin starts hook
     */
    afterStart() {
        const me = this;

        console.log(`Plugin has been started. Subscribed: ${me.isSubscribed}. Topic: ${me.topic}`);

        me.sendMessage(MessageBuilder.health());

        me.initCassandra().then(cassandra => {
            me.cassandra = cassandra;
            console.log('Cassandra schemas created');
        }).catch(me.onError);
    }

    /**
     * Message handler
     * @param message
     */
    handleMessage(message) {
        switch (message.type) {
            case MessageUtils.HEALTH_CHECK_TYPE:
                console.log(`NEW HEALTH CHECK TYPE MESSAGE. Payload: ${message.payload}`);
                break;
            case MessageUtils.TOPIC_TYPE:
                console.log(`NEW TOPIC TYPE MESSAGE. Payload: ${message.payload}`);
                break;
            case MessageUtils.PLUGIN_TYPE:
                console.log(`NEW PLUGIN TYPE MESSAGE. Payload: ${message.payload}`);
                break;
            case MessageUtils.NOTIFICATION_TYPE:
                try {
                    const messageBody = JSON.parse(message.payload.message).b;

                    if (messageBody.notification) {
                        console.log(`NEW NOTIFICATION TYPE MESSAGE. Notification: ${JSON.stringify(messageBody.notification)}`);
                        this.cassandra.insertNotification(messageBody.notification);
                    } else if (messageBody.command) {
                        if (messageBody.command.isUpdated) {
                            console.log(`NEW COMMAND UPDATE TYPE MESSAGE. Command: ${JSON.stringify(messageBody.command)}`);
                            if (cassandraConfig.CUSTOM.COMMAND_UPDATES_STORING) {
                                this.cassandra.insertCommandUpdate(messageBody.command)
                            }
                        } else {
                            console.log(`NEW COMMAND TYPE MESSAGE. Command: ${JSON.stringify(messageBody.command)}`);
                            this.cassandra.insertCommand(messageBody.command);
                        }
                    } else {
                        console.log(`UNKNOWN MESSAGE. Body: ${messageBody}`);
                    }
                } catch (error) {
                    console.log(`UNEXPECTED ERROR`);
                    console.log(error);
                }
                break;
            default:
                console.log(`UNKNOWN MESSAGE TYPE`);
                console.log(message);
                break;
        }
    }

    /**
     * Before plugin stops hook
     */
    beforeStop() {
        console.log(`Plugin will stop soon`);
    }

    /**
     * Plugin error handler
     */
    onError(error) {
        console.warn(`PLUGIN ERROR: ${error}`);
    }

    initCassandra() {
        const cassandra = CassandraStorage.connect(cassandraConfig);

        cassandra.assignTablesToCommands(...cassandraTables.commandTables);
        cassandra.assignTablesToCommandUpdates(...cassandraTables.commandUpdatesTables);
        cassandra.assignTablesToNotifications(...cassandraTables.notificationTables);

        return cassandra.initializeUDTSchemas(cassandraUDTs).then(() => {
            return cassandra.initializeTableSchemas(cassandraTables.tables);
        }).then(() => cassandra);
    }
}

module.exports = CassandraPluginService;