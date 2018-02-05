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

    ensureSchemasExist(cassandra) {
        return new Promise((resolve, reject) => {
            const schemaCheck = this._createSchemaChecking(cassandra);
            const checking = setInterval(() => {
                schemaCheck().then(ok => {
                    if (ok) {
                        clearInterval(checking);
                        resolve(cassandra);
                    }
                }).catch(err => {
                    clearInterval(checking);
                    reject(err);
                });
            }, +cassandraConfig.CUSTOM.SCHEMA_CHECKS_INTERVAL);
        });
    }

    _createSchemaChecking(cassandra) {
        let checkNumber = 0;
        const checksThreshold = +cassandraConfig.CUSTOM.SCHEMA_CHECKS_COUNT;
        return () => {
            return new Promise((resolve, reject) => {
                if (checkNumber >= checksThreshold) {
                    reject(new Error('CASSANDRA SCHEMAS HAVE NOT BEEN CREATED'));
                    return;
                }

                checkNumber++;
                cassandra.checkSchemasExistence(resolve);
            });
        };
    }
}

module.exports = CassandraPluginService;