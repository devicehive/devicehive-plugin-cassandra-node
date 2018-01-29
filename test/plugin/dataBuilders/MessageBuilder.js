const DataBuilder = require('smp-data-builder');
const { MessageUtils } = require(`devicehive-proxy-message`);

class MessageBuilder extends DataBuilder {
    withCommand(command) {
        return this.with('type', MessageUtils.NOTIFICATION_TYPE).withMessageData({ command });
    }

    withNotification(notification) {
        return this.with('type', MessageUtils.NOTIFICATION_TYPE).withMessageData({ notification });
    }

    withMessageData(data) {
        return this.with('payload', {
            message: JSON.stringify({
                b: data
            })
        });
    }
}

module.exports = MessageBuilder;