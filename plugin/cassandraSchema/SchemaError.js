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

    static schemaMismatch() {
        return new SchemaError(null, 'Schema mismatch, please check your JSON schemas of tables and actual schemas in Casasndra');
    }
}

module.exports = SchemaError;