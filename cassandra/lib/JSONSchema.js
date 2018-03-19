const Utils = require('./Utils');
const CassandraUtils = require('./CassandraUtils');
const Metadata = require('./metadata');

class JSONSchema {
    static get PRIMARY_KEY() { return '__primaryKey__'; }
    static get CLUSTERING_KEY() { return '__clusteringKey__'; }
    static get ORDER() { return '__order__'; }
    static get OPTIONS() { return '__options__'; }
    static get DROP_IF_EXISTS() { return '__dropIfExists__'; }

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
     * Constructs primary and clustering key definition
     * @returns {string}
     */
    buildKeys() {
        if (JSONSchema.invalidPrimaryKey(this._schema)) {
            return '';
        }

        const primaryKeyColumns = this._schema[JSONSchema.PRIMARY_KEY].join(',');
        let primaryKeyDefinition = `(${primaryKeyColumns})`;

        if (JSONSchema.validClusteringKey(this._schema)) {
            const clusteringKeyColumns = this._schema[JSONSchema.CLUSTERING_KEY].join(',');
            primaryKeyDefinition += `,${clusteringKeyColumns}`;
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
                    filteredObj[prop] = CassandraUtils.cassandraStringTypeOrDefault(this._schema[prop], obj[prop]);
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
        const primaryKeys = this._schema[JSONSchema.PRIMARY_KEY] || [];
        const clusteringKeys = this._schema[JSONSchema.CLUSTERING_KEY] || [];

        return primaryKeys.includes(prop) || clusteringKeys.includes(prop);
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
            if (JSONSchema.isNotReservedProperty(colName)) {
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
     * @param metadataDescriptor cassandra-driver metadata object
     * @returns {boolean}
     */
    comparePropertySetWithMetadata(metadataDescriptor) {
        return Metadata.create(metadataDescriptor).isSameMembersSchema(this);
    }

    /**
     * Returns true if schema contains same primary key as metadata
     * @param metadataDescriptor
     * @returns {boolean|*}
     */
    comparePrimaryKeyWithMetadata(metadataDescriptor) {
        return Metadata.create(metadataDescriptor).isSamePrimaryKey(this);
    }

    /**
     * Returns true if schema contains same clustering key as metadata
     * @param metadataDescriptor
     * @returns {boolean|*}
     */
    compareClusteringKeyWithMetadata(metadataDescriptor) {
        return Metadata.create(metadataDescriptor).isSameClusteringKey(this);
    }

    /**
     * Returns true if schema contains same ordering as metadata
     * @param metadataDescriptor
     * @returns {boolean|*}
     */
    compareOrderingWithMetadata(metadataDescriptor) {
        return Metadata.create(metadataDescriptor).isSameOrdering(this);
    }

    /**
     * Returns array of property types mismatches in schema with metadata
     * @param metadataDescriptor cassandra-driver metadata object
     * @returns {Array}
     */
    diffPropertyTypesWithMetadata(metadataDescriptor) {
        const mismatches = [];
        const metadata = Metadata.create(metadataDescriptor);

        const props = this.getProperties();

        for (let propName in props) {
            if (metadata.columnExists(propName)) {
                const realType = metadata.getFullTypeName(propName);
                const schemaType = CassandraUtils.replaceTypeAliases(props[propName].replace(/\s/g, ''));

                if (realType !== schemaType) {
                    mismatches.push({
                        propName,
                        realType,
                        schemaType
                    });
                }
            }
        }

        return mismatches;
    }

    /**
     * Returns schema properties and values which are not reserved
     * @returns {Object}
     */
    getProperties() {
        const cols = {};

        for (let prop in this._schema) {
            if (JSONSchema.isNotReservedProperty(prop)) {
                cols[prop] = this._schema[prop];
            }
        }

        return cols;
    }

    /**
     * Returns primary key columns if primary key is valid
     * @returns {Array}
     */
    getPrimaryKey() {
        return JSONSchema.validPrimaryKey(this._schema) ? [].concat(this._schema[JSONSchema.PRIMARY_KEY]) : [];
    }

    /**
     * Returns clustering key columns if clustering key is valid
     * @returns {Array}
     */
    getClusteringKey() {
        return JSONSchema.validClusteringKey(this._schema) ? [].concat(this._schema[JSONSchema.CLUSTERING_KEY]) : [];
    }

    getOrder() {
        return Utils.copy(this._schema[JSONSchema.ORDER]);
    }

    /**
     * Returns true if schema contains __dropIfExists__ set in true
     * @returns {boolean}
     */
    shouldBeDropped() {
        const dropIfExists = this._schema[JSONSchema.DROP_IF_EXISTS];
        return !!Utils.booleanOrDefault(dropIfExists);
    }

    /**
     * Returns true if property name is not specified as reserved
     * @param propName
     * @returns {boolean}
     */
    static isNotReservedProperty(propName) {
        const reservedProps = [
            JSONSchema.PRIMARY_KEY,
            JSONSchema.CLUSTERING_KEY,
            JSONSchema.ORDER,
            JSONSchema.OPTIONS,
            JSONSchema.DROP_IF_EXISTS
        ];

        return !reservedProps.includes(propName);
    }

    /**
     * Returns true if primary key of schema is invalid
     * @param schema
     * @returns {boolean}
     */
    static invalidPrimaryKey(schema) {
        return !JSONSchema.validPrimaryKey(schema);
    }

    /**
     * Returns true if primary key of schema is valid
     * @param schema
     * @returns {boolean}
     */
    static validPrimaryKey(schema) {
        return Utils.isNotEmpty(schema[JSONSchema.PRIMARY_KEY]);
    }

    /**
     * Returns true if clustering key of schema is valid
     * @param schema
     * @returns {boolean}
     */
    static validClusteringKey(schema) {
        return Utils.isNotEmpty(schema[JSONSchema.CLUSTERING_KEY]);
    }
}

module.exports = JSONSchema;