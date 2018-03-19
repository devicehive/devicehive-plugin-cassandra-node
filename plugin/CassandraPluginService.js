const PluginService = require('./PluginService');
const cassandraTables = require('../cassandraSchemas/cassandra-tables');
const cassandraUDTs = require('../cassandraSchemas/cassandra-user-types');
const cassandraInit = require('./cassandraInit');
const Utils = require('./Utils');

/**
 * Cassandra Plugin main class
 */
class CassandraPluginService extends PluginService {
    constructor(cassandraConfig) {
        if (Utils.isNotObject(cassandraConfig)) {
            throw new TypeError('First argument of CassandraPluginService constructor must be Cassandra config plain object');
        }

        super();

        this._cassandraConf = cassandraConfig;
        this._enableCommandUpdatesStoring = cassandraConfig.custom.commandUpdatesStoring;
    }

    /**
     * After plugin starts hook
     */
    afterStart() {
        super.afterStart();

        const schemas = {
            udts: cassandraUDTs,
            tables: cassandraTables
        };

        return this.initCassandra(this._cassandraConf, schemas).then(cassandra => {
            this.cassandra = cassandra;
        }).catch(err => {
            this.onError(err);
            process.exit(1);
        });
    }

    handleCommand(command) {
        super.handleCommand(command);
        return this.cassandra.insertCommand(command);
    }

    handleCommandUpdate(command) {
        super.handleCommandUpdate(command);
        if (this.isCommandUpdatesStoringEnabled()) {
            return this.cassandra.insertCommandUpdate(command);
        }

        return this.cassandra.updateCommand(command);
    }

    handleNotification(notification) {
        super.handleNotification(notification);
        return this.cassandra.insertNotification(notification);
    }

    initCassandra(conf, { udts, tables }) {
        return cassandraInit(conf, { udts, tables });
    }

    isCommandUpdatesStoringEnabled() {
        return Boolean(this._enableCommandUpdatesStoring);
    }
}

module.exports = CassandraPluginService;