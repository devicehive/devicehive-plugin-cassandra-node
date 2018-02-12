const DataBuilder = require('smp-data-builder');

class MetadataBuilder extends DataBuilder {
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

    withColumn(name, typeCode, info = {}) {
        return this.with('columnsByName', {
            [name]: {
                type: {
                    code: typeCode,
                    info
                }
            }
        });
    }
}

module.exports = MetadataBuilder;