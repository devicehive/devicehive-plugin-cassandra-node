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

        this.initCassandra().then(cassandra => {
            this.cassandra = cassandra;
            console.log('Cassandra connection initialized');
        }).catch(err => {
            this.onError(err);
            process.exit(1);
        });
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
        return CassandraStorage.connect(cassandraConfig).then(cassandra => {
            cassandra.setUDTSchemas(cassandraUDTs)
                .setTableSchemas(cassandraTables.tables)
                .assignTablesToCommands(...cassandraTables.commandTables)
                .assignTablesToCommandUpdates(...cassandraTables.commandUpdatesTables)
                .assignTablesToNotifications(...cassandraTables.notificationTables);

            return this.ensureSchemasExist(cassandra);
        });
    }

    // @TODO Split to smaller methods
    ensureSchemasExist(cassandra) {
        return new Promise((resolve, reject) => {
            let checkNumber = 0;
            const checking = setInterval(() => {
                if (checkNumber >= +cassandraConfig.CUSTOM.SCHEMA_CHECKS_COUNT) {
                    clearInterval(checking);
                    reject(new Error('CASSANDRA SCHEMAS HAVE NOT BEEN CREATED'));
                    return;
                }

                checkNumber++;
                cassandra.checkAllSchemasExist(exist => {
                    if (exist) {
                        resolve(cassandra);
                        clearInterval(checking);
                    }
                });
            }, +cassandraConfig.CUSTOM.SCHEMA_CHECKS_INTERVAL);
        });
    }
}

module.exports = CassandraPluginService;