import Result "mo:base/Result";
import Nat32 "mo:base/Nat32";
import Text "mo:base/Text";
import Buffer "mo:base/Buffer";
import Map "mo:map/Map";

module Collection {
    type Result<T, E> = Result.Result<T, E>;
    type PrimaryKey = Nat32;

    public type FieldValue = {
        #bool : Bool;
        #nat32 : Nat32;
        #nat32Null : ?Nat32;
        #text : Text;
        #textNull : ?Text;
    };

    type SchemaField = {
        name : Text;
        dataType : {
            #bool : {
                defaultValue : ?Bool;
            };
            #nat32 : {
                defaultValue : ?Nat32;
            };
            #nat32Null : {
                defaultValue : ?Nat32;
            };
            #text : {
                defaultValue : ?Text;
            };
            #textNull : {
                defaultValue : ?Text;
            };
        };
    };

    public type Schema = Map.Map<Text, SchemaField>;
    public type Values = [(name : Text, val : FieldValue)];
    public type SchemaValidationError = (fieldName : Text, msg : Text);

    public class Collection() {
        public let schema : Schema = Map.new();
        public let data = Map.new<Nat32, Values>();
        public var idCounter : Nat32 = 0;
    };

    public func addSchemaField(
        collection : Collection,
        field : SchemaField,
    ) : Result<(), { #FieldAlreadyExists }> {
        switch (Map.get(collection.schema, Map.thash, field.name)) {
            case (null) {
                ignore Map.put(collection.schema, Map.thash, field.name, field);
                #ok;
            };
            case (?_) {
                #err(#FieldAlreadyExists);
            };
        };
    };

    public func addSchemaFields(
        collection : Collection,
        fields : [SchemaField],
    ) : Result<(), [(fieldName : Text, err : { #FieldAlreadyExists })]> {
        let errors = Buffer.Buffer<(fieldName : Text, err : { #FieldAlreadyExists })>(0);

        for (field in fields.vals()) {
            let result = addSchemaField(collection, field);
            switch result {
                case (#ok) {};
                case (#err(err)) {
                    errors.add((field.name, err));
                };
            };
        };

        if (errors.size() > 0) {
            return #err(Buffer.toArray<(Text, { #FieldAlreadyExists })>(errors));
        };

        return #ok;
    };

    public func insert(
        collection : Collection,
        input : Values,
    ) : Result.Result<PrimaryKey, { #SchemaValidationError : [SchemaValidationError] }> {
        collection.idCounter += 1;
        let pk = collection.idCounter;
        // Add the primary key to the input
        let inputBuf = Buffer.fromArray<(Text, FieldValue)>(input);
        inputBuf.add(("id", #nat32(pk)));
        let finalInput = Buffer.toArray(inputBuf);
        switch (validateInputAgainstSchema(finalInput, collection.schema)) {
            case (#err(err)) { return #err(err) };
            case (#ok) {};
        };
        ignore Map.put(collection.data, Map.n32hash, pk, finalInput);
        return #ok(pk);
    };

    public func delete(collection : Collection, id : PrimaryKey) : () {
        Map.delete(collection.data, Map.n32hash, id);
    };

    public func find(collection : Collection, id : PrimaryKey) : ?Values {
        let result = Map.get(collection.data, Map.n32hash, id);
        switch result {
            case (null) { return null };
            case (val) { return val };
        };
    };

    public func filter(
        collection : Collection,
        predicate : (pk : PrimaryKey, item : Values) -> Bool,
    ) : [(PrimaryKey, Values)] {
        var result = Buffer.Buffer<(PrimaryKey, Values)>(0);
        for ((pk, item) in Map.entries(collection.data)) {
            if (predicate(pk, item)) {
                result.add((pk, item));
            };
        };
        return Buffer.toArray(result);
    };

    public func update(
        collection : Collection,
        id : PrimaryKey,
        input : Values,
    ) : Result.Result<(), { #NotFound }> {
        let existing = find(collection, id);

        switch (existing) {
            case (?_) {
                ignore Map.put(collection.data, Map.n32hash, id, input);
                #ok;
            };
            case (null) { #err(#NotFound) };
        };
    };

    public func getFieldSchema(collection : Collection, fieldName : Text) : ?SchemaField {
        return Map.get(collection.schema, Map.thash, fieldName);
    };

    public func validateInputAgainstSchema(
        input : [(Text, FieldValue)],
        schema : Schema,
    ) : Result<(), { #SchemaValidationError : [SchemaValidationError] }> {
        let values = Map.fromIter<Text, FieldValue>(
            input.vals(),
            Map.thash,
        );
        let validationErrors = Buffer.Buffer<SchemaValidationError>(
            input.size()
        );

        // Validate input against schema
        label _loop for ((fieldName, field) in Map.entries(schema)) {
            let value = Map.get(values, Map.thash, fieldName);

            switch (value) {
                case (null) {
                    switch (field.dataType) {
                        case (#bool(conf)) {
                            let defaultValue : Bool = switch (conf.defaultValue) {
                                case (null) {
                                    validationErrors.add((fieldName, "Missing required field"));
                                    continue _loop;
                                };
                                case (?x) { x };
                            };
                            ignore Map.put(values, Map.thash, fieldName, #bool(defaultValue));
                        };
                        case (#nat32(conf)) {
                            let defaultValue : Nat32 = switch (conf.defaultValue) {
                                case (null) {
                                    validationErrors.add((fieldName, "Missing required field"));
                                    continue _loop;
                                };
                                case (?x) { x };
                            };
                            ignore Map.put(values, Map.thash, fieldName, #nat32(defaultValue));
                        };
                        case (#nat32Null(conf)) {
                            ignore Map.put(values, Map.thash, fieldName, #nat32Null(conf.defaultValue));
                        };
                        case (#text(conf)) {
                            let defaultValue : Text = switch (conf.defaultValue) {
                                case (null) {
                                    validationErrors.add((fieldName, "Missing required field"));
                                    continue _loop;
                                };
                                case (?x) { x };
                            };
                            ignore Map.put(values, Map.thash, fieldName, #text(defaultValue));
                        };
                        case (#textNull(conf)) {
                            ignore Map.put(values, Map.thash, fieldName, #textNull(conf.defaultValue));
                        };
                    };
                };
                case (?value) {
                    switch (field.dataType) {
                        case (#bool(_)) {
                            switch (value) {
                                case (#bool(_)) {};
                                case (_) {
                                    validationErrors.add((fieldName, "Expected bool"));
                                };
                            };
                        };
                        case (#nat32(_)) {
                            switch (value) {
                                case (#nat32(_)) {};
                                case (_) {
                                    validationErrors.add((fieldName, "Expected nat32"));
                                };
                            };
                        };
                        case (#nat32Null(_)) {
                            switch (value) {
                                case (#nat32Null(_)) {};
                                case (_) {
                                    validationErrors.add((fieldName, "Expected nat32Null"));
                                };
                            };
                        };
                        case (#text(_)) {
                            switch (value) {
                                case (#text(_)) {};
                                case (_) {
                                    validationErrors.add((fieldName, "Expected text"));
                                };
                            };
                        };
                        case (#textNull(_)) {
                            switch (value) {
                                case (#textNull(_)) {};
                                case (_) {
                                    validationErrors.add((fieldName, "Expected textNull"));
                                };
                            };
                        };
                    };
                };
            };
        };

        if (validationErrors.size() > 0) {
            return #err(#SchemaValidationError(Buffer.toArray<SchemaValidationError>(validationErrors)));
        };

        return #ok;
    };
};
