class SchemaError extends Error {
    constructor(tableName, field, message) {
        super();

        this.tableName = tableName;
        this.message = message;
    }

    toString() {
        const prefix = this.tableName ? `SchemaError in table ${this.tableName}: ` : '';
        return prefix + this.message;
    }

    static parametersError(tableName) {
        return new SchemaError(tableName, 'parameters', 'parameters field is not allowed type');
    }
}

module.exports = SchemaError;