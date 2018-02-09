const Utils = require('./Utils');
const CassandraUtils = require('./CassandraUtils');

class JSONSchema {
    static get PRIMARY_KEY() { return '__primaryKey__'; }
    static get CLUSTERED_KEY() { return '__clusteredKey__'; }
    static get ORDER() { return '__order__'; }
    static get OPTIONS() { return '__options__'; }

    constructor(schema = {}) {
        this._schema = Utils.copy(schema);
    }

    /**
     * Constructs comma-separated column names and types
     * @returns {string}
     */
    buildColumnsDefinition() {
        const columns = [];

        for (let columnName in this._schema) {
            const col = this.buildColumn(columnName);
            if (col) {
                columns.push(col);
            }
        }

        return columns.join(',');
    }

    /**
     * Constructs definition of one column with type
     * @param columnName
     * @returns {string}
     */
    buildColumn(columnName) {
        if (this._schema.hasOwnProperty(columnName) && JSONSchema.isNotReservedProperty(columnName)) {
            return `${columnName} ${this._schema[columnName]}`;
        }

        return '';
    }

    /**
     * Constructs primary and clustered key definition
     * @returns {string}
     */
    buildKeys() {
        if (JSONSchema.invalidPrimaryKey(this._schema)) {
            return '';
        }

        const primaryKeyColumns = this._schema[JSONSchema.PRIMARY_KEY].join(',');
        let primaryKeyDefinition = `(${primaryKeyColumns})`;

        if (JSONSchema.validClusteredKey(this._schema)) {
            const clusteredKeyColumns = this._schema[JSONSchema.CLUSTERED_KEY].join(',');
            primaryKeyDefinition += `,${clusteredKeyColumns}`;
        }


        return `PRIMARY KEY(${primaryKeyDefinition})`;
    }

    /**
     * Constructs table options with table ordering
     * @returns {string}
     */
    buildTableConfiguration() {
        const ordering = this.buildOrderDefinition();
        const options = this.buildOptions();
        const tableConfig = [ordering, options].filter(Utils.isNotEmpty);

        return Utils.isNotEmpty(tableConfig) ? `WITH ${tableConfig.join(' AND ')}` : '';
    }

    /**
     * Constructs table ordering
     * @returns {string}
     */
    buildOrderDefinition() {
        const orderingRules = this._schema[JSONSchema.ORDER] || {};
        const orderBy = [];

        for (let columnName in orderingRules) {
            if(orderingRules.hasOwnProperty(columnName)) {
                orderBy.push(`${columnName} ${orderingRules[columnName]}`);
            }
        }

        return orderBy.length ? `CLUSTERING ORDER BY(${orderBy.join(',')})` : '';
    }

    /**
     * Constructs table options
     * @returns {string}
     */
    buildOptions() {
        const options = this._schema[JSONSchema.OPTIONS] || {};
        const optionStrings = [];

        for (const name in options) {
            if (options.hasOwnProperty(name)) {
                const value = JSONSchema.cassandraOptionValue(options[name]);
                optionStrings.push(`${name} = ${value}`);
            }
        }

        return optionStrings.join(' AND ');
    }

    /**
     * Format value to Cassandra option value
     * @param {any} value
     * @returns {string | any}
     */
    static cassandraOptionValue(value) {
        return JSON.stringify(value).replace(/"/g, '\'');
    }

    /**
     * Filters object properties based on schema
     * @param obj
     * @returns {Object | null}
     */
    filterData(obj) {
        if (!obj) {
            return null;
        }

        const filteredObj = {};

        for (let prop in this._schema) {
            if (this._schema.hasOwnProperty(prop) && JSONSchema.isNotReservedProperty(prop) && prop in obj) {
                const isUDT = Utils.isObject(this._schema[prop]);
                if (isUDT) {
                    const udtSchema = new JSONSchema(this._schema[prop]);
                    filteredObj[prop] = udtSchema.filterData(obj[prop]);
                } else {
                    filteredObj[prop] = JSONSchema.cassandraStringTypeOrDefault(this._schema[prop], obj[prop]);
                }
            }
        }

        return filteredObj;
    }

    extractNotKeys(data) {
        const keys = this.extractKeys(data);
        if (!keys) {
            return null;
        }

        const notKeys = { ...data };

        Object.keys(keys).forEach(k => {
            if (k in notKeys) {
                delete notKeys[k];
            }
        });

        return notKeys;
    }

    extractKeys(data) {
        if (!data) {
            return null;
        }

        const keys = {};

        for (let prop in data) {
            if (data.hasOwnProperty(prop) && this.isKey(prop)) {
                keys[prop] = data[prop];
            }
        }

        return keys;
    }

    isKey(prop) {
        const primaryKeys = this._schema[JSONSchema.PRIMARY_KEY];
        const clusteredKeys = this._schema[JSONSchema.CLUSTERED_KEY];

        return primaryKeys.includes(prop) || clusteredKeys.includes(prop);
    }

    /**
     * Cast to string if Cassandra column is text, varchar or ascii type
     * @param {string} type
     * @param {any} val
     * @returns {string | any}
     */
    static cassandraStringTypeOrDefault(type, val = null) {
        const stringTypes = [ 'text', 'varchar', 'ascii' ];

        if (stringTypes.includes(type) && val !== null) {
            return Utils.isObject(val) ? JSON.stringify(val) : val.toString();
        }

        return val;
    }

    /**
     * Replaces current user type references in column definitions with real objects of user type definitions
     * @param {object} types
     * @returns {JSONSchema}
     */
    fillWithTypes(types) {
        if (!types) {
            return this;
        }

        for (let colName in this._schema) {
            if (this._schema.hasOwnProperty(colName) && JSONSchema.isNotReservedProperty(colName)) {
                const typeName = CassandraUtils.extractTypeName(this._schema[colName]);
                if (types[typeName]) {
                    this._schema[colName] = types[typeName];
                }
            }
        }

        return this;
    }

    /**
     * Returns true if schema contains same columns as metadata
     * @param metadata cassandra-driver metadata object
     * @returns {boolean}
     */
    compareColumnsSetWithMetadata(metadata) {
        const schemaColumns = Object.keys(this.getColumns()).map(col => col.toLowerCase());
        const realTableColumns = Object.keys(metadata.columnsByName);

        if (schemaColumns.length !== realTableColumns.length) {
            return false;
        }

        return schemaColumns.every(col => realTableColumns.includes(col));
    }

    /**
     * Returns array of column types mismatches in schema with metadata
     * @param metadata cassandra-driver metadata object
     * @returns {Array}
     */
    diffColumnTypesWithMetadata(metadata) {
        const mismatches = [];

        const columns = this.getColumns();

        for (let colName in columns) {
            if (colName in metadata.columnsByName) {
                const { type: dataType } = metadata.columnsByName[colName];
                const realType = CassandraUtils.fullTypeName(dataType);
                const schemaType = columns[colName].replace(/\s/g, '');

                if (realType !== schemaType) {
                    mismatches.push({
                        colName,
                        realType,
                        schemaType
                    });
                }
            }
        }

        return mismatches;
    }

    /**
     * Returns schema properties ad values which are not reserved
     * @returns {Object}
     */
    getColumns() {
        const cols = {};

        for (let prop in this._schema) {
            if (JSONSchema.isNotReservedProperty(prop)) {
                cols[prop] = this._schema[prop];
            }
        }

        return cols;
    }

    /**
     * Returns true if property name is not specified as reserved
     * @param propName
     * @returns {boolean}
     */
    static isNotReservedProperty(propName) {
        const reservedProps = [
            JSONSchema.PRIMARY_KEY,
            JSONSchema.CLUSTERED_KEY,
            JSONSchema.ORDER,
            JSONSchema.OPTIONS
        ];

        return !reservedProps.includes(propName);
    }

    /**
     * Returns true if primary key of schema is invalid
     * @param schema
     * @returns {boolean}
     */
    static invalidPrimaryKey(schema) {
        return !schema.hasOwnProperty(JSONSchema.PRIMARY_KEY) || !schema[JSONSchema.PRIMARY_KEY].length;
    }

    /**
     * Returns true if clustered key of schema is valid
     * @param schema
     * @returns {boolean}
     */
    static validClusteredKey(schema) {
        return schema.hasOwnProperty(JSONSchema.CLUSTERED_KEY) && schema[JSONSchema.CLUSTERED_KEY].length;
    }
}

module.exports = JSONSchema;