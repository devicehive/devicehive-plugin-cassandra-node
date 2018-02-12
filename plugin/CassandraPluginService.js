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
        } else {
            this.cassandra.updateCommand(command);
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

            return this._schemaComparison(cassandra);
        }).then(cassandra => this.ensureSchemasExist(cassandra));
    }

    ensureSchemasExist(cassandra) {
        return new Promise((resolve, reject) => {
            const interval = Number(cassandraConfig.CUSTOM.SCHEMA_CHECKS_INTERVAL) || 1000;
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
            }, interval);
        });
    }

    _createSchemaChecking(cassandra) {
        let checkNumber = 0;
        const checksThreshold = Number(cassandraConfig.CUSTOM.SCHEMA_CHECKS_COUNT) || 0;
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

    _schemaComparison(cassandra) {
        const tableComparisonNotifier = cassandra.compareTableSchemas();
        const udtComparisonNotifier = cassandra.compareUDTSchemas();

        const tableComparison = new Promise((resolve, reject) => {
            let ok = true;

            tableComparisonNotifier.on('tableExists', tableName => {
                console.log(`TABLE ${tableName}: Table already exists`);
            }).on('columnsMismatch', tableName => {
                console.log(`TABLE ${tableName}: Mismatched schema`);
                ok = false;
            }).on('columnTypesMismatch', (tableName, colName, realType, schemaType) => {
                console.log(`TABLE ${tableName}: Mismatched ${colName} type, actual "${realType}", in JSON schema "${schemaType}"`);
                ok = false;
            }).on('done', () => {
                if (ok) {
                    resolve();
                } else {
                    reject(SchemaError.tableSchemaMismatch());
                }
            });
        });

        const udtComparison = new Promise((resolve, reject) => {
            let ok = true;

            udtComparisonNotifier.on('customTypeExists', udtName => {
                console.log(`UDT ${udtName}: UDT already exists`);
            }).on('fieldsMismatch', udtName => {
                console.log(`UDT ${udtName}: Mismatched schema`);
                ok = false;
            }).on('fieldTypesMismatch', (udtName, fieldName, realType, schemaType) => {
                console.log(`UDT ${udtName}: Mismatched ${fieldName} type, actual "${realType}", in JSON schema "${schemaType}"`);
                ok = false;
            }).on('done', () => {
                if (ok) {
                    resolve();
                } else {
                    reject(SchemaError.udtSchemaMismatch());
                }
            });
        });

        return Promise.all([ tableComparison, udtComparison ]);
    }
}

module.exports = CassandraPluginService;