const EventEmitter = require('events');

class SchemaComparisonNotifier extends EventEmitter {
    constructor(events) {
        super();
        this._events = events;
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
}

module.exports = SchemaComparisonNotifier;