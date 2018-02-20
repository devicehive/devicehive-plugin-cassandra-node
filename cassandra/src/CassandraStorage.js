const Utils = require('../lib/Utils');
const JSONSchema = require('../lib/JSONSchema');
const CQLBuilder = require('../lib/CQLBuilder');

const SchemaComparisonNotifier = require('./SchemaComparisonNotifier');

class CassandraStorage {
    static get COMMAND_GROUP() { return 'commands'; }
    static get NOTIFICATION_GROUP() { return 'notifications'; }
    static get COMMAND_UPDATES_GROUP() { return 'commandUpdates'; }

    constructor(cassandraClient) {
        this._cassandra = cassandraClient;
        this._tableGroups = new Map();
        this._tableSchemas = null;
        this._userTypes = null;
    }

    /**
     * Creates table schemas in Cassandra DB
     * @param {object} tables
     * @returns {Promise<any[]>} All results of executed queries
     */
    createTableSchemas(tables) {
        this.setTableSchemas(tables);
        return this._createSchemas(CQLBuilder.createTable(), tables);
    }

    /**
     * Creates User Defined Types in Cassandra DB
     * @param {object} types
     * @returns {Promise<any[]>} All results of executed queries
     */
    createUDTSchemas(types) {
        this.setUDTSchemas(types);
        return this._createSchemas(CQLBuilder.createUDT(), types);
    }

    /**
     * Executes structure creation queries with specified query builder and schemaDescriptions
     * @param {BaseSchemaBuilder} schemaQueryBuilder
     * @param {object} schemaDescriptions
     * @returns {Promise<any[]>} All results of executed queries
     * @private
     */
    _createSchemas(schemaQueryBuilder, schemaDescriptions) {
        const execution = [];

        for (let name in schemaDescriptions) {
            if (schemaDescriptions.hasOwnProperty(name)) {
                const query = schemaQueryBuilder
                    .withName(name)
                    .fromJSONSchema(schemaDescriptions[name])
                    .ifNotExists()
                    .build();

                if (Utils.isNotEmpty(query)) {
                    execution.push(this._cassandra.execute(query))
                }
            }
        }

        return execution.length ? Promise.all(execution) : Promise.resolve(null);
    }

    dropTableSchemas(tables) {
        return this._dropSchemas(CQLBuilder.dropTable().ifExists(), tables);
    }

    dropTypeSchemas(types) {
        return this._dropSchemas(CQLBuilder.dropType().ifExists(), types);
    }

    _dropSchemas(queryBuilder, schemas) {
        const execution = [];

        for (let name in schemas) {
            const schema = new JSONSchema(schemas[name]);
            if (schema.shouldBeDropped()) {
                const query = queryBuilder.withName(name).build();
                execution.push(this._cassandra.execute(query));
            }
        }

        return execution.length ? Promise.all(execution) : Promise.resolve(null);
    }

    /**
     * Assigns tables to command group
     * @param tableNames
     * @returns {CassandraStorage}
     */
    assignTablesToCommands(...tableNames) {
        this._tableGroups.set(CassandraStorage.COMMAND_GROUP, tableNames);
        return this;
    }

    /**
     * Assigns tables to notification group
     * @param tableNames
     * @returns {CassandraStorage}
     */
    assignTablesToNotifications(...tableNames) {
        this._tableGroups.set(CassandraStorage.NOTIFICATION_GROUP, tableNames);
        return this;
    }

    /**
     * Assigns tables to command updates group
     * @param tableNames
     * @returns {CassandraStorage}
     */
    assignTablesToCommandUpdates(...tableNames) {
        this._tableGroups.set(CassandraStorage.COMMAND_UPDATES_GROUP, tableNames);
        return this;
    }

    /**
     * Inserts command into each table which is assigned to command group
     * @param commandData
     * @returns {Promise<any>}
     */
    insertCommand(commandData) {
        return this._insertIntoTableGroup(CassandraStorage.COMMAND_GROUP, commandData);
    }

    /**
     * Inserts notification into each table which is assigned to notification group
     * @param notificationData
     * @returns {Promise<any>}
     */
    insertNotification(notificationData) {
        return this._insertIntoTableGroup(CassandraStorage.NOTIFICATION_GROUP, notificationData);
    }

    /**
     * Inserts command update into each table which is assigned to command updates group
     * @param commandUpdateData
     * @returns {Promise<any>}
     */
    insertCommandUpdate(commandUpdateData) {
        return this._insertIntoTableGroup(CassandraStorage.COMMAND_UPDATES_GROUP, commandUpdateData);
    }

    /**
     * Updates command in each table which is assigned to command group
     * @param commandData
     * @returns {Promise<any>}
     */
    updateCommand(commandData) {
        return this._updateTableGroup(CassandraStorage.COMMAND_GROUP, commandData);
    }

    /**
     * Inserts data into specified group of tables
     * @param {string} groupName
     * @param {Object} data
     * @returns {Promise<any>}
     * @private
     */
    _insertIntoTableGroup(groupName, data) {
        const queryBuilder = this._createQueryBuilderWithData(CQLBuilder.INSERT_QUERY, data);
        return this._queryTableGroup(groupName, queryBuilder);
    }

    _updateTableGroup(groupName, data) {
        const queryBuilder = this._createQueryBuilderWithData(CQLBuilder.UPDATE_QUERY, data);
        return this._queryTableGroup(groupName, queryBuilder);
    }

    _createQueryBuilderWithData(type, data) {
        return CQLBuilder.query(type).queryParams(data).withJSONCustomTypes(this._userTypes);
    }

    _queryTableGroup(groupName, queryBuilder) {
        const tablesToInsert = this._tableGroups.get(groupName) || [];
        const queries = [];

        tablesToInsert.forEach(table => {
            const schema = this._tableSchemas[table];
            if (!schema) {
                return;
            }

            const q = queryBuilder.table(table, this._cassandra.keyspace).withJSONSchema(schema);

            queries.push(q.build());
        });

        return queries.length ? this._cassandra.batch(queries, { prepare: true }) : Promise.resolve(null);
    }

    /**
     * Sets table schemas for query operations
     * @param {object} value
     * @returns {CassandraStorage}
     */
    setTableSchemas(value) {
        this._tableSchemas = value;
        return this;
    }

    /**
     * Sets UDT (user defined type) schemas for query operations
     * @param {object} value
     * @returns {CassandraStorage}
     */
    setUDTSchemas(value) {
        this._userTypes = value;
        return this;
    }

    /**
     * Executes given callback with 'true' if all set schemas exist and 'false' in other case
     * @param {function} callback
     * @returns {CassandraStorage}
     */
    checkSchemasExistence(callback) {
        const tableSchemasCheck = this._requestMetadata(this._tableSchemas);
        const udtSchemasCheck = this._requestMetadata(this._userTypes);

        Promise.all(tableSchemasCheck.concat(udtSchemasCheck)).then(results => {
            const allSchemasExist = results.every(r => r);
            callback(allSchemasExist);
        });

        return this;
    }

    compareTableSchemas() {
        const notifier = new SchemaComparisonNotifier({
            exists: 'tableExists',
            structureMismatch: 'columnsMismatch',
            typesMismatch: 'columnTypesMismatch'
        });

        return this._compareSchemas(this._tableSchemas, notifier);
    }

    compareUDTSchemas() {
        const notifier = new SchemaComparisonNotifier({
            exists: 'customTypeExists',
            structureMismatch: 'fieldsMismatch',
            typesMismatch: 'fieldTypesMismatch'
        });

        return this._compareSchemas(this._userTypes, notifier);
    }

    _compareSchemas(schemas, notifier) {
        const requests = this._requestMetadata(schemas);

        requests.forEach(metadataRequest => {
            metadataRequest.then(md => {
                if (!md) {
                    return;
                }

                const { name } = md;

                notifier.notifyExistence(name);

                const schema = new JSONSchema(schemas[name]);

                if (!schema.comparePropertySetWithMetadata(md)) {
                    notifier.notifyStructureMismatch(name);
                }

                schema.diffPropertyTypesWithMetadata(md).forEach(mismatch => {
                    const mismatchDetails = [ name, mismatch.propName, mismatch.realType, mismatch.schemaType ];
                    notifier.notifyTypesMismatch(...mismatchDetails);
                });

                if (!schema.comparePrimaryKeyWithMetadata(md)) {
                    notifier.notifyPrimaryKeyMismatch(name);
                }

                if (!schema.compareClusteringKeyWithMetadata(md)) {
                    notifier.notifyClusteringKeyMismatch(name);
                }
            });
        });

        Promise.all(requests).then(() => {
            notifier.emit('done');
        });

        return notifier;
    }

    /**
     * Creates array of metadata requests
     * @param {object} schemas this._tableSchemas or this._userTypes object
     * @returns {Array}
     * @private
     */
    _requestMetadata(schemas) {
        if (!schemas) {
            return [];
        }

        let method = '';
        if (schemas === this._tableSchemas) {
            method = 'getTable';
        } else if (schemas === this._userTypes) {
            method = 'getUdt';
        } else {
            return [];
        }

        const requests = [];
        const ks = this._cassandra.keyspace;
        for (const name in schemas) {
            if (schemas.hasOwnProperty(name)) {
                requests.push(this._cassandra.metadata[method](ks, name));
            }
        }

        return requests;
    }
}

module.exports = CassandraStorage;