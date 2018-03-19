class SchemaError extends Error {
    constructor(tableName, message) {
        super();

        this.tableName = tableName;
        this.message = message;
    }

    toString() {
        const prefix = this.tableName ? `SchemaError in table ${this.tableName}: ` : '';
        return prefix + this.message;
    }

    static parametersError(tableName) {
        return new SchemaError(tableName, 'parameters field is not allowed type');
    }

    static tableSchemaMismatch() {
        return new SchemaError(null, 'Table schema mismatch, please check your JSON schema of tables and actual schemas in Cassandra');
    }

    static udtSchemaMismatch() {
        return new SchemaError(null, 'UDT schema mismatch, please check your JSON schema of UDTs and actual schemas in Cassandra');
    }
}

module.exports = SchemaError;