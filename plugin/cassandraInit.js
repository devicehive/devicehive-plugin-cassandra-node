const cassandraTables = require('../cassandraSchemas/cassandra-tables');
const cassandraUDTs = require('../cassandraSchemas/cassandra-user-types');
const CassandraStorage = require('../cassandra');
const SchemaValidator = require('./cassandraSchema/SchemaValidator');

module.exports = (cassandraConfig) => {
    const errors = SchemaValidator.getSchemasErrors(cassandraTables.tables);

    if (errors.length) {
        return Promise.reject(errors);
    }

    return CassandraStorage.connect(cassandraConfig).then(cassandra => {
        cassandra.setUDTSchemas(cassandraUDTs)
            .setTableSchemas(cassandraTables.tables)
            .assignTablesToCommands(...cassandraTables.commandTables)
            .assignTablesToCommandUpdates(...cassandraTables.commandUpdatesTables)
            .assignTablesToNotifications(...cassandraTables.notificationTables);

        return schemaComparison(cassandra);
    }).then(cassandra => ensureSchemasExist(cassandra, cassandraConfig));
};

function ensureSchemasExist(cassandra, cassandraConfig) {
    return new Promise((resolve, reject) => {
        const interval = Number(cassandraConfig.CUSTOM.SCHEMA_CHECKS_INTERVAL) || 1000;
        const schemaCheck = createSchemaChecking(cassandra, cassandraConfig);
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

function createSchemaChecking(cassandra, cassandraConfig) {
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

function schemaComparison(cassandra) {
    const tableComparisonNotifier = cassandra.compareTableSchemas();
    const udtComparisonNotifier = cassandra.compareUDTSchemas();
    const errorMsg = 'Schema mismatch, please check your JSON schemas of UDTs, tables and actual schemas in Cassandra';

    const tableComparison = new Promise((resolve, reject) => {
        let ok = true;

        tableComparisonNotifier.on('columnsMismatch', tableName => {
            console.log(`TABLE ${tableName}: Mismatched schema`);
            ok = false;
        }).on('columnTypesMismatch', (tableName, colName, realType, schemaType) => {
            console.log(`TABLE ${tableName}: Mismatched ${colName} type, actual "${realType}", in JSON schema "${schemaType}"`);
            ok = false;
        }).on('primaryKeyMismatch', tableName => {
            console.log(`TABLE ${tableName}: Mismatched primary key`);
            ok = false;
        }).on('clusteringKeyMismatch', tableName => {
            console.log(`TABLE ${tableName}: Mismatched clustering key`);
            ok = false;
        }).on('done', () => {
            if (ok) {
                resolve();
            } else {
                reject(new Error(errorMsg));
            }
        });
    });

    const udtComparison = new Promise((resolve, reject) => {
        let ok = true;

        udtComparisonNotifier.on('fieldsMismatch', udtName => {
            console.log(`UDT ${udtName}: Mismatched schema`);
            ok = false;
        }).on('fieldTypesMismatch', (udtName, fieldName, realType, schemaType) => {
            console.log(`UDT ${udtName}: Mismatched ${fieldName} type, actual "${realType}", in JSON schema "${schemaType}"`);
            ok = false;
        }).on('done', () => {
            if (ok) {
                resolve();
            } else {
                reject(new Error(errorMsg));
            }
        });
    });

    return Promise.all([ tableComparison, udtComparison ]).then(() => cassandra);
}