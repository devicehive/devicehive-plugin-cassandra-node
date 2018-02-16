const DataBuilder = require('smp-data-builder');

class TablesBuilder extends DataBuilder {
    withTables(...tableNames) {
        tableNames.forEach(name => {
            this.with(name, {
                col1: 'int',
                col2: 'text',
                col3: 'varchar',
                __primaryKey__: [ 'col1' ],
                __clusteringKey__: [ 'col2' ]
            });
        });

        return this;
    }

    withSchemaForAll(schema) {
        for (let tableName in this.obj) {
            this.obj[tableName] = schema;
        }

        return this;
    }
}

module.exports = TablesBuilder;