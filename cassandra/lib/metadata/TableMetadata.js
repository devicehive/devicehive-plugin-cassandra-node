const Metadata = require('./Metadata');

class TableMetadata extends Metadata {
    isSameMembersSchema(jsonSchema) {
        const columns = Object.keys(this._md.columnsByName);
        return this._sameStructure(jsonSchema, columns);
    }

    columnExists(colName) {
        colName = colName.toLowerCase();
        return colName in this._md.columnsByName;
    }

    _getTypeDescription(colName) {
        return this._md.columnsByName[colName].type;
    }

    isSamePrimaryKey(jsonSchema) {
        const jsonSchemaPrimaryKey = jsonSchema.getPrimaryKey().map(pk => pk.toLowerCase());
        const metadataPrimaryKey = (this._md.partitionKeys || []).map(pk => pk.name);

        if (jsonSchemaPrimaryKey.length !== metadataPrimaryKey.length) {
            return false;
        }

        return jsonSchemaPrimaryKey.every(pk => metadataPrimaryKey.includes(pk));
    }
}

module.exports = TableMetadata;