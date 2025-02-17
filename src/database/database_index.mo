import List "mo:base/List";
import RBTree "mo:base/RBTree";
import Text "mo:base/Text";

module DatabaseIndex {
    type Data<PrimaryKeyT> = RBTree.RBTree<Text, List.List<PrimaryKeyT>>;
    type PrimaryKeyList<PrimaryKeyT> = List.List<PrimaryKeyT>;

    /**
    * Represents a database index. An index is a mapping from an attribute
    * value to a list of primary keys. The primary key is used to retrieve
    * the actual object from the database.
    **/
    public class DatabaseIndex<PrimaryKeyT>(
        options : {
            name : Text;
        }
    ) {
        public let name = options.name;
        private let _data = RBTree.RBTree<Text, List.List<PrimaryKeyT>>(Text.compare);

        public func get(attr_value : Text) : List.List<PrimaryKeyT> {
            var ids = _data.get(attr_value);

            return switch (ids) {
                case (null) { List.fromArray<PrimaryKeyT>([]) };
                case (?ids) { ids };
            };
        };

        public func put(attr_value : Text, pk : PrimaryKeyT) : () {
            let pks = get(attr_value);
            _data.put(attr_value, List.push(pk, pks));
        };

    };
};
