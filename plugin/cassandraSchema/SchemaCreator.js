const SchemaError = require('./SchemaError');

class SchemaCreator {
    constructor(cassandraClient) {
        this._client = cassandraClient;
    }

    create({ udt, tables }) {
        const tableComparisonNotifier = this._client.setTableSchemas(tables).compareTableSchemas();
        const udtComparisonNotifier = this._client.setUDTSchemas(udt).compareUDTSchemas();
        const comparison = this._resolveSchemaComparison(tableComparisonNotifier, udtComparisonNotifier);

        return comparison.then(() => this._client.createUDTSchemas(udt))
            .then(() => this._client.createTableSchemas(tables));
    }

    dropBeforeCreate({ udt, tables }) {
        return this._client.dropTableSchemas(tables).then(() => this._client.dropTypeSchemas(udt));
    }

    _resolveSchemaComparison(tableComparisonNotifier, udtComparisonNotifier) {
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
            }).on('primaryKeyMismatch', tableName => {
                console.log(`TABLE ${tableName}: Mismatched primary key`);
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

module.exports = SchemaCreator;