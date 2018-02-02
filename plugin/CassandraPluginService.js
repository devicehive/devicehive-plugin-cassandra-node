const PluginService = require('./PluginService');

const cassandraConfig = require(`./config`).cassandra;
const cassandraTables = require('../cassandraSchemas/cassandra-tables');
const cassandraUDTs = require('../cassandraSchemas/cassandra-user-types');
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

        this.cassandra = this.initCassandra();
    }

    handleCommand(command) {
        super.handleCommand(command);
        this.cassandra.insertCommand(command);
    }

    handleCommandUpdate(command) {
        super.handleCommandUpdate(command);
        if (cassandraConfig.CUSTOM.COMMAND_UPDATES_STORING) {
            this.cassandra.insertCommandUpdate(command);
        }
    }

    handleNotification(notification) {
        super.handleNotification(notification);
        this.cassandra.insertNotification(notification);
    }

    initCassandra() {
        const cassandra = CassandraStorage.connect(cassandraConfig);

        cassandra.assignTablesToCommands(...cassandraTables.commandTables);
        cassandra.assignTablesToCommandUpdates(...cassandraTables.commandUpdatesTables);
        cassandra.assignTablesToNotifications(...cassandraTables.notificationTables);

        return cassandra.setUDTSchemas(cassandraUDTs).setTableSchemas(cassandraTables.tables);
    }
}

module.exports = CassandraPluginService;