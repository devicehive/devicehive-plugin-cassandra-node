const EventEmitter = require('events');

class SchemaComparisonNotifier extends EventEmitter {
    constructor(events) {
        super();
        this._notificationEvents = { ...events };
        this._notificationEvents.primaryKeyMismatch = 'primaryKeyMismatch';
        this._notificationEvents.clusteringKeyMismatch = 'clusteringKeyMismatch';
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

    notifyClusteringKeyMismatch(name) {
        return this.emit(this._notificationEvents.clusteringKeyMismatch, name);
    }
}

module.exports = SchemaComparisonNotifier;