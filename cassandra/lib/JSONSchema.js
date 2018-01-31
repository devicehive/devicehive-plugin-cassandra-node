const Utils = require('./Utils');
const CassandraUtils = require('../src/CassandraUtils');

class JSONSchema {
    static get PRIMARY_KEY() { return '__primaryKey__'; }
    static get CLUSTERED_KEY() { return '__clusteredKey__'; }
    static get ORDER() { return '__order__'; }

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
            if(this._schema.hasOwnProperty(columnName)) {
                const col = this.buildColumn(columnName);
                if(col) {
                    columns.push(col);
                }
            }
        }

        return columns.join(',');
    }

    /**
     * Construct definition of one column with type
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
        if(JSONSchema.invalidPrimaryKey(this._schema)) {
            return '';
        }

        const primaryKeyColumns = this._schema[JSONSchema.PRIMARY_KEY].join(',');
        let primaryKeyDefinition = `(${primaryKeyColumns})`;

        if(JSONSchema.validClusteredKey(this._schema)) {
            const clusteredKeyColumns = this._schema[JSONSchema.CLUSTERED_KEY].join(',');
            primaryKeyDefinition += `,${clusteredKeyColumns}`;
        }


        return `PRIMARY KEY(${primaryKeyDefinition})`;
    }

    buildOrderDefinition() {
        const orderingRules = this._schema[JSONSchema.ORDER];
        const orderBy = [];

        for (let columnName in orderingRules) {
            if(orderingRules.hasOwnProperty(columnName)) {
                orderBy.push(`${columnName} ${orderingRules[columnName]}`);
            }
        }

        return orderBy.length ? `WITH CLUSTERING ORDER BY(${orderBy.join(',')})` : '';
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
                if (Utils.isObject(this._schema[prop])) {
                    filteredObj[prop] = new JSONSchema(this._schema[prop]).filterData(obj[prop]);
                } else {
                    filteredObj[prop] = JSONSchema.cassandraStringTypeOrDefault(this._schema[prop], obj[prop]);
                }
            }
        }

        return filteredObj;
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
     * Returns true if property name is not specified as reserved
     * @param propName
     * @returns {boolean}
     */
    static isNotReservedProperty(propName) {
        const reservedProps = [
            JSONSchema.PRIMARY_KEY,
            JSONSchema.CLUSTERED_KEY,
            JSONSchema.ORDER
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