const PluginService = require('./PluginService');
const cassandraTables = require('../cassandraSchemas/cassandra-tables');
const cassandraUDTs = require('../cassandraSchemas/cassandra-user-types');
const cassandraInit = require('./cassandraInit');

/**
 * Cassandra Plugin main class
 */
class CassandraPluginService extends PluginService {
    constructor(cassandraConfig) {
        if (typeof cassandraConfig !== 'object') {
            throw new TypeError('First argument of CassandraPluginService constructor must be Cassandra config object');
        }

        super();

        this._cassandraConf = cassandraConfig;
        this._enableCommandUpdatesStoring = cassandraConfig.CUSTOM.COMMAND_UPDATES_STORING;
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

        this.initCassandra(this._cassandraConf, schemas).then(cassandra => {
            this.cassandra = cassandra;
            console.log('Cassandra connection initialized');
        }).catch(err => {
            this.onError(err);
            process.exit(1);
        });
    }

    handleCommand(command) {
        super.handleCommand(command);
        this.cassandra.insertCommand(command);
    }

    handleCommandUpdate(command) {
        super.handleCommandUpdate(command);
        if (this.isCommandUpdatesStoringEnabled()) {
            this.cassandra.insertCommandUpdate(command);
        } else {
            this.cassandra.updateCommand(command);
        }
    }

    handleNotification(notification) {
        super.handleNotification(notification);
        this.cassandra.insertNotification(notification);
    }

    initCassandra(conf, { udts, tables }) {
        return cassandraInit(conf, { udts, tables });
    }

    isCommandUpdatesStoringEnabled() {
        return Boolean(this._enableCommandUpdatesStoring);
    }
}

module.exports = CassandraPluginService;