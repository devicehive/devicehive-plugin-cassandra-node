const Utils = require('../lib/Utils');
const CQLBuilder = require('../lib/CQLBuilder');

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
     * Inserts data into specified group of tables
     * @param {string} groupName
     * @param {Object} data
     * @returns {Promise<any>}
     * @private
     */
    _insertIntoTableGroup(groupName, data) {
        const tablesToInsert = this._tableGroups.get(groupName) || [];
        const queries = [];

        tablesToInsert.forEach(table => {
            const schema = this._tableSchemas[table];
            if (!schema) {
                return;
            }

            const q = CQLBuilder.insertInto(table, this._cassandra.keyspace).queryParams(data);
            q.withJSONSchema(schema).withJSONCustomTypes(this._userTypes);

            queries.push(q.build());
        });

        return queries.length ? this._cassandra.batch(queries, { prepare: true }) : Promise.resolve(null);
    }

    updateCommand(commandData) {
        const tablesToInsert = this._tableGroups.get(CassandraStorage.COMMAND_GROUP) || [];
        const queries = [];

        tablesToInsert.forEach(table => {
            const schema = this._tableSchemas[table];
            if (!schema) {
                return;
            }

            const q = CQLBuilder.update(table, this._cassandra.keyspace).queryParams(commandData);
            q.withJSONSchema(schema).withJSONCustomTypes(this._userTypes);

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