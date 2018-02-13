const PluginService = require('./PluginService');
const cassandraInit = require('./cassandraInit');

const cassandraConfig = require(`./config`).cassandra;

/**
 * Cassandra Plugin main class
 */
class CassandraPluginService extends PluginService {

    /**
     * After plugin starts hook
     */
    afterStart() {
        super.afterStart();

        this.initCassandra().then(cassandra => {
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
        if (cassandraConfig.CUSTOM.COMMAND_UPDATES_STORING) {
            this.cassandra.insertCommandUpdate(command);
        } else {
            this.cassandra.updateCommand(command);
        }
    }

    handleNotification(notification) {
        super.handleNotification(notification);
        this.cassandra.insertNotification(notification);
    }

    initCassandra() {
        return cassandraInit();
    }
}

module.exports = CassandraPluginService;