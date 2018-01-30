const { DeviceHivePlugin } = require(`devicehive-plugin-core`);
const { MessageBuilder, MessageUtils } = require(`devicehive-proxy-message`);

/**
 * Base Plugin class for this service
 */
class PluginService extends DeviceHivePlugin {
    /**
     * Before plugin starts hook
     */
    beforeStart() {
        console.log(`Plugin is starting`);
    }

    /**
     * After plugin starts hook
     */
    afterStart() {
        const me = this;

        console.log(`Plugin has been started. Subscribed: ${me.isSubscribed}. Topic: ${me.topic}`);

        me.sendMessage(MessageBuilder.health());
    }

    /**
     * Message handler
     * @param message
     */
    handleMessage(message) {
        switch (message.type) {
            case MessageUtils.HEALTH_CHECK_TYPE:
                console.log(`NEW HEALTH CHECK TYPE MESSAGE. Payload: ${message.payload}`);
                break;
            case MessageUtils.TOPIC_TYPE:
                console.log(`NEW TOPIC TYPE MESSAGE. Payload: ${message.payload}`);
                break;
            case MessageUtils.PLUGIN_TYPE:
                console.log(`NEW PLUGIN TYPE MESSAGE. Payload: ${message.payload}`);
                break;
            case MessageUtils.NOTIFICATION_TYPE:
                try {
                    const messageBody = JSON.parse(message.payload.message).b;

                    if (messageBody.notification) {
                        this.handleNotification(messageBody.notification);
                    } else if (messageBody.command) {
                        if (messageBody.command.isUpdated) {
                            this.handleCommandUpdate(messageBody.command);
                        } else {
                            this.handleCommand(messageBody.command);
                        }
                    } else {
                        console.log(`UNKNOWN MESSAGE. Body: ${messageBody}`);
                    }
                } catch (error) {
                    console.log(`UNEXPECTED ERROR`);
                    console.log(error);
                }
                break;
            default:
                console.log(`UNKNOWN MESSAGE TYPE`);
                console.log(message);
                break;
        }
    }

    /**
     * Notification message handler
     * @param notification
     */
    handleNotification(notification) {
        console.log(`NEW NOTIFICATION TYPE MESSAGE. Notification: ${JSON.stringify(notification)}`);
    }

    /**
     * Command update message handler
     * @param command
     */
    handleCommandUpdate(command) {
        console.log(`NEW COMMAND UPDATE TYPE MESSAGE. Command: ${JSON.stringify(command)}`);
    }

    /**
     * Command message handler
     * @param command
     */
    handleCommand(command) {
        console.log(`NEW COMMAND TYPE MESSAGE. Command: ${JSON.stringify(command)}`);
    }

    /**
     * Before plugin stops hook
     */
    beforeStop() {
        console.log(`Plugin will stop soon`);
    }

    /**
     * Plugin error handler
     */
    onError(error) {
        console.warn(`PLUGIN ERROR: ${error}`);
    }
}

module.exports = PluginService;