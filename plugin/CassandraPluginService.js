const PluginService = require('./PluginService');

const cassandraConfig = require(`./config`).cassandra;
const cassandraTables = require('./cassandra-tables');
const cassandraUDTs = require('./cassandra-user-types');
const CassandraStorage = require('../cassandra');

/**
 * Cassandra Plugin main class
 */
class CassandraPluginService extends PluginService {

    /**
     * After plugin starts hook
     */
    afterStart() {
        super.afterStart();

        this.initCassandra().then(cassandra => {
            this.cassandra = cassandra;
            console.log('Cassandra schemas created');
        }).catch(this.onError);
    }

    handleCommand(command) {
        this.cassandra.insertCommand(command);
    }

    handleCommandUpdate(command) {
        this.cassandra.insertCommandUpdate(command);
    }

    handleNotification(notification) {
        this.cassandra.insertNotification(notification);
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