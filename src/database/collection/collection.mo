import HashMap "mo:base/HashMap";
import Result "mo:base/Result";
import Debug "mo:base/Debug";
import Nat32 "mo:base/Nat32";
import Buffer "mo:base/Buffer";
import QuerySet "../query_set";

module Collection {
    type Result<T, E> = Result.Result<T, E>;
    type PrimaryKey = Nat32;

    public class Collection<T>() {
        public let data = HashMap.HashMap<Nat32, T>(0, Nat32.equal, func(x) { x });
        public var idCounter : Nat32 = 0;
    };

    public func add<T>(
        collection : Collection<T>,
        input : T,
    ) : PrimaryKey {
        collection.idCounter += 1;
        let pk = collection.idCounter;
        collection.data.put(pk, input);
        return pk;
    };

    public func delete<T>(collection : Collection<T>, id : PrimaryKey) : () {
        collection.data.delete(id);
    };

    public func find<T>(collection : Collection<T>, id : PrimaryKey) : ?T {
        let result = collection.data.get(id);
        switch result {
            case (null) { return null };
            case (?block) { return ?block };
        };
    };

    public func filter<T>(
        collection : Collection<T>,
        predicate : (pk : PrimaryKey, item : T) -> Bool,
    ) : [(PrimaryKey, T)] {
        var result = Buffer.Buffer<(PrimaryKey, T)>(0);
        for ((pk, item) in collection.data.entries()) {
            if (predicate(pk, item)) {
                result.add((pk, item));
            };
        };
        return Buffer.toArray(result);
    };

    public func get<T>(collection : Collection<T>, id : PrimaryKey) : T {
        let block = find(collection, id);
        switch block {
            case (?block) { return block };
            case (null) { Debug.trap("Item not found: " # debug_show id) };
        };
    };

    public func update<T>(
        collection : Collection<T>,
        id : PrimaryKey,
        input : T,
    ) : Result.Result<(), { #NotFound }> {
        let existing = find(collection, id);

        switch (existing) {
            case (?_) {
                collection.data.put(id, input);
                #ok;
            };
            case (null) { #err(#NotFound) };
        };
    };

    public func q<T>() : QuerySet.QuerySet<T> {
        return QuerySet.QuerySet<T>(null);
    };
};
