class CassandraStorageLogger {
    constructor(log) {
        this._log = log;
        this._loggingHooks = this._createLoggingHooks();
    }

    attach(cassandraStorage) {
        const logMethods = [
            '_execute', '_batchExecute', 'createTableSchemas', 'createUDTSchemas',
            'dropTableSchemas', 'dropTypeSchemas', 'checkSchemasExistence', 'compareTableSchemas',
            '_queryTableGroup'
        ];

        logMethods.forEach(methodName => {
            cassandraStorage[methodName] = this._wrapMethod(cassandraStorage, methodName);
        });
    }

    _wrapMethod(cassandraStorage, methodName) {
        const method = cassandraStorage[methodName];
        const log = this._loggingHooks[methodName];

        return function(...args) {
            log.before && log.before(...args);

            const res = method.apply(cassandraStorage, args);

            if (res instanceof Promise) {
                res.then((...results) => {
                    log.afterSuccess && log.afterSuccess(...args, ...results);
                }).catch(err => {
                    log.afterError && log.afterError(...args, err);
                    return Promise.reject(err);
                });
            }

            return res;
        };
    }

    _createLoggingHooks() {
        const me = this;
        return {
            _execute: {
                before(query) {
                    me._log(`Executing ${query}`);
                },

                afterSuccess(query, res) {
                    me._log(`Executed successfully: ${query}`);
                },

                afterError(query, err) {
                    me._log(`Query failed: ${query}; Error: ${err.message}`);
                }
            },

            _batchExecute: {
                before(queries) {
                    me._log(`Executing batch ${JSON.stringify(queries)}`);
                },

                afterSuccess(queries, res) {
                    me._log(`Batched queries executed successfully ${JSON.stringify(queries)}`);
                },

                afterError(queries, err) {
                    me._log(`Batched queries failed ${JSON.stringify(queries)}; Error: ${err.message}`);
                }
            },

            createTableSchemas: {
                before(tables) {
                    const tablesCount = Object.keys(tables).length;
                    me._log(`Creating ${tablesCount} table${tablesCount > 1 ? 's' : ''}`);
                }
            },

            createUDTSchemas: {
                before(types) {
                    const typesCount = Object.keys(types).length;
                    me._log(`Creating ${typesCount} user defined type${typesCount > 1 ? 's' : ''}`);
                }
            },

            dropTableSchemas: {
                before(tables) {
                    const tablesCount = Object.keys(tables).length;
                    me._log(`Will try to drop ${tablesCount} table schema${tablesCount > 1 ? 's' : ''}`);
                }
            },

            dropTypeSchemas: {
                before(types) {
                    const typesCount = Object.keys(types).length;
                    me._log(`Will try to drop ${typesCount} user defined type schema${typesCount > 1 ? 's' : ''}`);
                }
            },

            checkSchemasExistence: {
                before() {
                    me._log('Checking schema existence');
                }
            },

            compareTableSchemas: {
                before() {
                    me._log('Comparing table schemas');
                }
            },

            _queryTableGroup: {
                before(groupName, queryBuilder) {
                    me._log(`Querying table group: ${groupName}`);
                }
            }
        };
    }
}

module.exports = CassandraStorageLogger;