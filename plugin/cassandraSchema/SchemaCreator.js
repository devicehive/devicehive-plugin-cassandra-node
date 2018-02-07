const SchemaError = require('./SchemaError');

class SchemaCreator {
    constructor(cassandraClient) {
        this._client = cassandraClient;
    }

    create({ udt, tables }) {
        return this._client.createUDTSchemas(udt).then(() => this._client.createTableSchemas(tables));
    }

    static getSchemasErrors(tableSchemas) {
        const errors = [];

        for (const tableName in tableSchemas) {
            if (tableSchemas.hasOwnProperty(tableName)) {
                const err = SchemaCreator.getSchemaError(tableSchemas[tableName], tableName);

                if (err) {
                    errors.push(err);
                }
            }
        }

        return errors;
    }

    static getSchemaError(tableSchema, tableName) {
        const paramsValid = SchemaCreator._validateParametersField(tableSchema);
        return paramsValid ? null : SchemaError.parametersError(tableName);
    }

    static _validateParametersField(tableSchema) {
        if (tableSchema.parameters) {
            const isNotString = SchemaCreator._parametersIsNotString(tableSchema.parameters);
            const isNotMap = SchemaCreator._parametersIsNotMap(tableSchema.parameters);
            const isNotUDT = SchemaCreator._parametersIsNotUDT(tableSchema.parameters);

            if (isNotString && isNotMap && isNotUDT) {
                return false;
            }
        }

        return true;
    }

    static _parametersIsNotString(type) {
        return !SchemaCreator._allowedPrimitiveParametersTypes.includes(type);
    }

    static _parametersIsNotMap(type) {
        const mapOfStringsPattern = /(frozen<)?map<(text|ascii|varchar),\s?(text|ascii|varchar)>>?/i;
        return !type.match(mapOfStringsPattern);
    }

    static _parametersIsNotUDT(type) {
        return SchemaCreator._notAllowedParametersTypes.some(t => {
            const primitiveTypePattern = new RegExp(`<(\\w+\\,\\s?)?${t}(\\,\\s?\\w+)?>`, 'i');
            const complexTypePattern = new RegExp(`${t}<[\\w\\,]*>`);

            const isPrimitive = Boolean(type.match(primitiveTypePattern));
            const isComplex = Boolean(type.match(complexTypePattern));

            return type === t || isPrimitive || isComplex;
        });
    }

    static get _notAllowedParametersTypes() {
        return SchemaCreator._cassandraTypes.filter(t => !SchemaCreator._allowedParametersTypes.includes(t));
    }

    static get _allowedParametersTypes() {
        return SchemaCreator._allowedPrimitiveParametersTypes.concat([ 'map' ]);
    }

    static get _allowedPrimitiveParametersTypes() {
        return [
            'text',
            'ascii',
            'varchar'
        ];
    }

    static get _cassandraTypes() {
        return [
            'ascii',
            'bigint',
            'blob',
            'boolean',
            'counter',
            'decimal',
            'double',
            'float',
            'int',
            'text',
            'timestamp',
            'uuid',
            'varchar',
            'varint',
            'timeuuid',
            'inet',
            'date',
            'time',
            'smallint',
            'tinyint',
            'list',
            'map',
            'set',
            'udt',
            'tuple'
        ];
    }
}

module.exports = SchemaCreator;