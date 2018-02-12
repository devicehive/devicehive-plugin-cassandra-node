const Metadata = require('./Metadata');

class TableMetadata extends Metadata {
    isSameMembersSchema(jsonSchema) {
        const columns = Object.keys(this._md.columnsByName);
        return this._sameStructure(jsonSchema, columns);
    }

    columnExists(colName) {
        return colName in this._md.columnsByName;
    }

    _getTypeDescription(colName) {
        return this._md.columnsByName[colName].type;
    }
}

module.exports = TableMetadata;