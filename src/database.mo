import Array "mo:base/Array";
import Buffer "mo:base/Buffer";
import Map "mo:map/Map";
import Nat "mo:base/Nat";
import Result "mo:base/Result";
import Text "mo:base/Text";
import Collection "collection";

module {
    type UniqueConstraint = {
        collection : Text;
        fields : [Text];
    };

    type ForeignKeyConstraint = {
        collection : Text;
        field : Text;
        targetCollection : Text; // The collection that the foreign key references
    };

    type Constraint = {
        #foreignKey : ForeignKeyConstraint;
        #unique : UniqueConstraint;
    };

    type Query = {
        fields : [{
            name : Text;
            alias : ?Text;
        }];
        where : [{
            field : Text;
            comparator : { #eq : Collection.FieldValue };
        }];
    };

    class QuerySet(values : Buffer.Buffer<(Nat32, Collection.Values)>) {
        public func value() : [(Nat32, Collection.Values)] {
            return Buffer.toArray<(Nat32, Collection.Values)>(values);
        };

        public func join(
            db : Database,
            collectionName : Text,
            conditions : [{ field : Text; comparator : { #eq : Text } }],
        ) : QuerySet {
            let collection = switch (Map.get(db.collections, Map.thash, collectionName)) {
                case (null) { return QuerySet(values) };
                case (?c) { c };
            };

            let newValues = Buffer.map<(Nat32, Collection.Values), (Nat32, Collection.Values)>(
                values,
                func(value) {
                    let pk = value.0;
                    let fields = value.1;
                    let updatedFields = Buffer.fromArray<(Text, Collection.FieldValue)>(fields);

                    for (row in Map.entries(collection.data)) {
                        let otherFields = row.1;
                        var isMatching = true;

                        label iterateConditions for (condition in conditions.vals()) {
                            let conditionField = condition.field;
                            let comparator = condition.comparator;

                            let indexInRowA : ?Nat = Array.indexOf<(Text, Collection.FieldValue)>(
                                (conditionField, #nat32(0)),
                                fields,
                                func(a, b) { a.0 == b.0 }, // Compare by field name
                            );

                            switch (indexInRowA) {
                                case (null) { continue iterateConditions };
                                case (?idxA) {
                                    let fieldA = fields[idxA];
                                    let fieldAValue = fieldA.1;

                                    switch (comparator) {
                                        case (#eq(comparatorField)) {
                                            let indexInRowB : ?Nat = Array.indexOf<(Text, Collection.FieldValue)>(
                                                (comparatorField, #nat32(0)),
                                                otherFields,
                                                func(a, b) { a.0 == b.0 }, // Compare by field name
                                            );

                                            switch (indexInRowB) {
                                                case (null) {
                                                    isMatching := false;
                                                    break iterateConditions;
                                                };
                                                case (?idxB) {
                                                    let fieldB = otherFields[idxB];
                                                    let fieldBValue = fieldB.1;

                                                    if (fieldAValue != fieldBValue) {
                                                        isMatching := false;
                                                        break iterateConditions;
                                                    };
                                                };
                                            };
                                        };
                                    };
                                };
                            };
                        };

                        if (isMatching) {
                            for ((fieldName, fieldValue) in otherFields.vals()) {
                                let fullName = collectionName # "." # fieldName;
                                updatedFields.add((fullName, fieldValue));
                            };
                        };
                    };

                    return (pk, Buffer.toArray(updatedFields));
                },
            );

            return QuerySet(newValues);
        };
    };

    public class Database() {
        public let collections = Map.new<Text, Collection.Collection>();
        public let constraints = Map.new<Text, Constraint>();
        public let indexedConstraints = Map.new<Text, Map.Map<Text, [Constraint]>>();
    };

    public func addCollection(
        database : Database,
        name : Text,
        collection : Collection.Collection,
    ) : Result.Result<(), { #CollectionAlreadyExists }> {
        switch (Map.get(database.collections, Map.thash, name)) {
            case (null) {
                ignore Map.put(database.collections, Map.thash, name, collection);
                #ok;
            };
            case (?_) {
                #err(#CollectionAlreadyExists);
            };
        };
    };

    public func dropCollection(
        database : Database,
        name : Text,
    ) : Result.Result<(), { #CollectionDoesNotExist }> {
        switch (Map.get(database.collections, Map.thash, name)) {
            case (null) {
                #err(#CollectionDoesNotExist);
            };
            case (?_) {
                ignore Map.remove(database.collections, Map.thash, name);
                #ok;
            };
        };
    };

    public func addConstraint(
        database : Database,
        name : Text,
        constraint : Constraint,
    ) : Result.Result<(), { #ConstraintAlreadyExists }> {
        switch (Map.get(database.constraints, Map.thash, name)) {
            case (null) {
                ignore Map.put(database.constraints, Map.thash, name, constraint);
                #ok;
            };
            case (?_) {
                #err(#ConstraintAlreadyExists);
            };
        };
    };

    public func dropConstraint(
        database : Database,
        name : Text,
    ) : Result.Result<(), { #ConstraintDoesNotExist }> {
        switch (Map.get(database.constraints, Map.thash, name)) {
            case (null) {
                #err(#ConstraintDoesNotExist);
            };
            case (?_) {
                ignore Map.remove(database.constraints, Map.thash, name);
                #ok;
            };
        };
    };

    public func insert(
        database : Database,
        collectionName : Text,
        input : Collection.Values,
    ) : Result.Result<Nat32, { #CollectionDoesNotExist; #SchemaValidationError : [Collection.SchemaValidationError] }> {
        let collection = switch (Map.get(database.collections, Map.thash, collectionName)) {
            case (null) { return #err(#CollectionDoesNotExist) };
            case (?c) { c };
        };

        // Validate each item in the input against the constraints
        for (val in Array.vals(input)) {
            let fieldName = val.0;
            let fieldValue = val.1;

            if (fieldName == "id") {
                return #err(#SchemaValidationError([(fieldName, "Field name 'id' is reserved")]));
            };

            let constraints = getConstraintsOnField(database, collectionName, fieldName);
            label iterateConstraints for (constraint in constraints.vals()) {
                switch (constraint) {
                    case (#foreignKey(constraint)) {
                        let typedFieldValue = switch (fieldValue) {
                            case (#nat32(_)) { continue iterateConstraints };
                            case (_) {
                                return #err(#SchemaValidationError([(fieldName, "Field with foreign key constraint is not a Nat32")]));
                            };
                        };
                        let targetCollection = switch (Map.get(database.collections, Map.thash, constraint.targetCollection)) {
                            case (null) {
                                return #err(#SchemaValidationError([(fieldName, "Target collection does not exist")]));
                            };
                            case (?c) { c };
                        };

                        // Check if the value exists in the target collection
                        switch (Collection.find(targetCollection, typedFieldValue)) {
                            case (null) {
                                return #err(#SchemaValidationError([(fieldName, "Field violates foreign key constraint")]));
                            };
                            case (?_) {};
                        };
                    };
                    case (#unique(_)) {
                        // TODO: Check if the value is unique
                    };
                };
            };
        };

        switch (Collection.insert(collection, input)) {
            case (#ok(pk)) { return #ok(pk) };
            case (#err(err)) { return #err(err) };
        };
    };

    public func select(
        database : Database,
        collectionName : Text,
        qry : Query,
    ) : Result.Result<QuerySet, { #CollectionDoesNotExist }> {
        let collection = switch (Map.get(database.collections, Map.thash, collectionName)) {
            case (null) { return #err(#CollectionDoesNotExist) };
            case (?c) { c };
        };

        let matchingRows = Buffer.Buffer<(Nat32, Collection.Values)>(0);

        // Do an extremely naive implementation of the query
        // TODO: Make this more performant by using indexes
        label iterateRows for ((pk, fields) in Map.entries(collection.data)) {
            var matches = true;

            label iterateWhereClause for (where in qry.where.vals()) {
                let comparator = where.comparator;

                // Find matching field value and compare
                for ((fieldName, fieldValue) in fields.vals()) {
                    if (fieldName == where.field) {
                        switch (comparator) {
                            case (#eq(value)) {
                                if (fieldValue != value) {
                                    matches := false;
                                    break iterateWhereClause;
                                };
                            };
                        };
                    };
                };
            };

            if (matches) {
                matchingRows.add((pk, fields));
            };
        };

        return #ok(QuerySet(matchingRows));
    };

    private func getConstraintsOnField(
        database : Database,
        collectionName : Text,
        fieldName : Text,
    ) : [Constraint] {
        return switch (Map.get(database.indexedConstraints, Map.thash, collectionName)) {
            case (null) { return [] };
            case (?collectionConstraints) {
                switch (Map.get(collectionConstraints, Map.thash, fieldName)) {
                    case (null) { return [] };
                    case (?c) { c };
                };
            };
        };
    };
};
