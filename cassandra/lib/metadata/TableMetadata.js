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
        return this._isSameKey('primary', jsonSchema);
    }

    isSameClusteringKey(jsonSchema) {
        return this._isSameKey('clustering', jsonSchema);
    }

    _isSameKey(keyType, jsonSchema) {
        let jsonSchemaKey;
        let metadataKey;

        if (keyType === 'primary') {
            jsonSchemaKey = jsonSchema.getPrimaryKey();
            metadataKey = this._md.partitionKeys;
        } else {
            jsonSchemaKey = jsonSchema.getClusteringKey();
            metadataKey = this._md.clusteringKeys;
        }

        jsonSchemaKey = jsonSchemaKey.map(k => k.toLowerCase());
        metadataKey = (metadataKey || []).map(k => k.name);

        if (jsonSchemaKey.length !== metadataKey.length) {
            return false;
        }

        return jsonSchemaKey.every(k => metadataKey.includes(k));
    }

    isSameOrdering(jsonSchema) {
        if (!this.isSameClusteringKey(jsonSchema)) {
            return false;
        }

        const mdClusteringKeys = this._md.clusteringKeys;
        const mdOrder = this._md.clusteringOrder;
        const schemaOrder = jsonSchema.getOrder();

        let valid = true;
        for (let i = 0; i < mdClusteringKeys.length; i++) {
            const ck = mdClusteringKeys[i];
            const schemaCKOrder = schemaOrder[ck.name] || 'ASC';

            valid = schemaCKOrder.toUpperCase().trim() === mdOrder[i].toUpperCase();
            if (!valid) {
                break;
            }
        }

        return valid;
    }
}

module.exports = TableMetadata;