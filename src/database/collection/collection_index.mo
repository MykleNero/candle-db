import Iter "mo:base/Iter";
import Text "mo:base/Text";
import Buffer "mo:base/Buffer";
import HashMap "mo:base/HashMap";


import Types "./types";

module CollectionIndex {
    type PrimaryKey = Types.PrimaryKey;

    type IndexKey = Text;
    type IndexValue = Buffer.Buffer<PrimaryKey>;

    /**
    * Represents a database index. An index is a mapping from an attribute
    * value to a list of primary keys. The primary keys are used to retrieve
    * the actual objects from the database.
    **/
    public class CollectionIndex() {
        public let data = HashMap.HashMap<IndexKey, IndexValue>(0, Text.equal, Text.hash);
    };

    public func get(index : CollectionIndex, key : IndexKey) : Iter.Iter<PrimaryKey> {
        var ids = switch (index.data.get(key)) {
            case (null) { Buffer.fromArray<PrimaryKey>([]) };
            case (?ids) { ids };
        };

        return ids.vals();
    };

    public func put(index : CollectionIndex, key : Text, pk : PrimaryKey) : () {
        switch (_valueAtKey(index, key)) {
            case (null) {
                let ids = Buffer.make<PrimaryKey>(pk);
                index.data.put(key, ids);
            };
            case (?ids) {
                if (Buffer.contains(ids, pk, Text.equal)) {
                    return;
                };

                ids.add(pk);
            };
        };
    };

    public func remove(index : CollectionIndex, key : Text, pk : PrimaryKey) : () {
        switch (_valueAtKey(index, key)) {
            case (null) { return };
            case (?ids) {
                ids.filterEntries(
                    func(_, id) { id != pk },
                );
            };
        };
    };

    private func _valueAtKey(index : CollectionIndex, key : IndexKey) : ?IndexValue {
        switch (index.data.get(key)) {
            case (null) { return null };
            case (?ids) { return ?ids };
        };
    };
};
