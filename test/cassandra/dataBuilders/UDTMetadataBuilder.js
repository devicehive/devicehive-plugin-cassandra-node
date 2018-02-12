const DataBuilder = require('smp-data-builder');

class UDTMetadataBuilder extends DataBuilder {
    withTextField(name) {
        const TEXT_TYPE_CODE = 10;
        return this.withField(name, TEXT_TYPE_CODE);
    }

    withIntField(name) {
        const INT_TYPE_CODE = 9;
        return this.withField(name, INT_TYPE_CODE);
    }

    withField(name, typeCode) {
        return this.with('fields', [ {
            type: {
                code: typeCode
            },
            name
        } ]);
    }

    withName(name) {
        return this.with('name', name);
    }
}

module.exports = UDTMetadataBuilder;