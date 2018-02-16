const EventEmitter = require('events');

class SchemaComparisonNotifier extends EventEmitter {
    constructor(events) {
        super();
        this._notificationEvents = { ...events };
        this._notificationEvents.primaryKeyMismatch = 'primaryKeyMismatch';
    }

    notifyExistence(name) {
        return this.emit(this._notificationEvents.exists, name);
    }

    notifyStructureMismatch(name) {
        return this.emit(this._notificationEvents.structureMismatch, name);
    }

    notifyTypesMismatch(...mismatchDetails) {
        return this.emit(this._notificationEvents.typesMismatch, ...mismatchDetails);
    }

    notifyPrimaryKeyMismatch(name) {
        return this.emit(this._notificationEvents.primaryKeyMismatch, name);
    }
}

module.exports = SchemaComparisonNotifier;