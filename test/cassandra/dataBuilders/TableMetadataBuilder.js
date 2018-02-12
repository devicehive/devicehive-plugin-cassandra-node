const DataBuilder = require('smp-data-builder');

class TableMetadataBuilder extends DataBuilder {
    withColumnTypeOption(columnName, optName, optVal) {
        const type = this.obj.columnsByName[columnName].type;

        if (!type.options) {
            type.options = {};
        }

        type.options[optName] = optVal;

        return this;
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