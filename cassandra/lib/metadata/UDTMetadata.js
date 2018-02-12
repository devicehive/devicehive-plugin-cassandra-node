const Metadata = require('./Metadata');

class UDTMetadata extends Metadata {
    isSameMembersSchema(jsonSchema) {
        const fields = this._md.fields.map(f => f.name);
        return this._sameStructure(jsonSchema, fields);
    }

    columnExists(colName) {
        return this._md.fields.some(f => f.name === colName);
    }

    _getTypeDescription(colName) {
        return this._md.fields.filter(f => f.name === colName)[0].type;
    }
}

module.exports = UDTMetadata;