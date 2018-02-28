const logger = require('winston');

const CassandraPluginService = require('./CassandraPluginService');

class CassandraPluginServiceLogger extends CassandraPluginService {
    constructor(cassandraConfig) {
        super(cassandraConfig);
        logger.level = cassandraConfig.custom.logLevel;
    }

    afterStart() {
        return super.afterStart().then(() => {
            logger.info('Cassandra connection initialized');
        });
    }

    handleCommand(command) {
        const stringCommand = JSON.stringify(command);

        return super.handleCommand(command).then(res => {
            if (res) {
                logger.debug(`Command insert success: ${stringCommand}`);
            } else {
                logger.warn(`Command hasn't been inserted, please check table group definitions: ${stringCommand}`);
            }
        }).catch(err => {
            logger.error(`Command insert fail: ${err}, ${stringCommand}`);
        });
    }

    handleCommandUpdate(command) {
        const stringCommand = JSON.stringify(command);

        return super.handleCommandUpdate(command).then(res => {
            if (res) {
                logger.debug(`Command update success: ${stringCommand}`);
            } else {
                logger.warn(`Command update was not processed, please check table group definitions: ${stringCommand}`);
            }
        }).catch(err => {
            logger.error(`Command update fail: ${err}, ${stringCommand}`);
        });
    }

    handleNotification(notification) {
        const stringNotification = JSON.stringify(notification);

        return super.handleCommandUpdate(notification).then(res => {
            if (res) {
                logger.debug(`Notification insert success: ${stringNotification}`);
            } else {
                logger.warn(`Notification hasn't been inserted, please check table group definitions: ${stringNotification}`);
            }
        }).catch(err => {
            logger.error(`Notification insert fail: ${err}, ${stringNotification}`);
        });
    }
}

module.exports = CassandraPluginServiceLogger;