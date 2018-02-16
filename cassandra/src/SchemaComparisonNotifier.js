const EventEmitter = require('events');

class SchemaComparisonNotifier extends EventEmitter {
    constructor(events) {
        super();
        this._events = { ...events };
        this._events.primaryKeyMismatch = 'primaryKeyMismatch';
    }

    notifyExistence(name) {
        return this.emit(this._events.exists, name);
    }

    notifyStructureMismatch(name) {
        return this.emit(this._events.structureMismatch, name);
    }

    notifyTypesMismatch(...mismatchDetails) {
        return this.emit(this._events.typesMismatch, ...mismatchDetails);
    }

    notifyPrimaryKeyMismatch(name) {
        return this.emit(this._events.primaryKeyMismatch, name);
    }
}

module.exports = SchemaComparisonNotifier;