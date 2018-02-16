const DataBuilder = require('smp-data-builder');

class TableMetadataBuilder extends DataBuilder {
    withPrimaryKey(...colNames) {
        colNames.forEach(col => {
            this.with('partitionKeys', [ { name: col } ]);
        });

        return this;
    }

    withColumnTypeOption(columnName, optName, optVal) {
        const type = this.obj.columnsByName[columnName].type;

        if (!type.options) {
            type.options = {};
        }

        type.options[optName] = optVal;

        return this;
    }

    withColumnNestedTextType(columnName) {
        const TEXT_TYPE_CODE = 10;
        return this.withColumnNestedType(columnName, TEXT_TYPE_CODE);
    }

    withColumnNestedType(columnName, typeCode) {
        const type = this.obj.columnsByName[columnName].type;

        if (!(type.info instanceof Array)) {
            type.info = [];
        }

        type.info.push({ code: typeCode });

        return this;
    }

    withColumnTypeName(columnName, typeName) {
        this.obj.columnsByName[columnName].type.info.name = typeName;
        return this;
    }

    withMapColumn(name) {
        const MAP_TYPE_CODE = 33;
        return this.withColumn(name, MAP_TYPE_CODE);
    }

    withUDTColumn(name) {
        const UDT_TYPE_CODE = 48;
        return this.withColumn(name, UDT_TYPE_CODE);
    }

    withTextColumn(name) {
        const TEXT_TYPE_CODE = 10;
        return this.withColumn(name, TEXT_TYPE_CODE);
    }

    withIntColumn(name) {
        const INT_TYPE_CODE = 9;
        return this.withColumn(name, INT_TYPE_CODE);
    }

    withColumn(name, typeCode, info = {}) {
        return this.with('columnsByName', {
            [name]: {
                type: {
                    code: typeCode,
                    info
                },
                name
            }
        });
    }

    withName(name) {
        return this.with('name', name);
    }
}

module.exports = TableMetadataBuilder;