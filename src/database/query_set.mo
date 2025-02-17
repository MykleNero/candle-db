import Bool "mo:base/Bool";
import List "mo:base/List";
import Order "mo:base/Order";
import Array "mo:base/Array";

module QuerySet {
    public class QuerySet<DataT>(data : ?[DataT]) {
        var initial_items = switch (data) {
            case (null) { [] };
            case (?data) { data };
        };

        private var _items = List.fromArray<DataT>(initial_items);

        public func filter(predicate : (item : DataT) -> Bool) : QuerySet<DataT> {
            let filtered_items = List.filter<DataT>(_items, predicate);
            return QuerySet<DataT>(?List.toArray<DataT>(filtered_items));
        };

        public func fromCursor(cursor : Nat, compare : (Nat, DataT) -> Bool) : QuerySet<DataT> {
            let index = findIndex<DataT>(
                func(page : DataT) : Bool {
                    return compare(cursor, page);
                }
            );
            switch index {
                case (null) { return QuerySet<DataT>(?[]) };
                case (?index) {
                    let from_cursor_items = List.drop<DataT>(_items, index);
                    return QuerySet<DataT>(?List.toArray<DataT>(from_cursor_items));
                };
            };
        };

        public func findIndex<AttrT>(predicate : (item : DataT) -> Bool) : ?Nat {
            var index = 0;
            for (n in List.toIter<DataT>(_items)) {
                if (predicate(n)) {
                    return ?index;
                };
                index := index + 1;
            };
            return null;
        };

        public func sort(
            predicate : (DataT, DataT) -> Order.Order
        ) : QuerySet<DataT> {
            var instances = List.fromArray<DataT>([]);
            for (item in List.toIter<DataT>(_items)) {
                instances := List.push<DataT>(item, instances);
            };
            return QuerySet<DataT>(?Array.sort<DataT>(List.toArray(instances), predicate));
        };

        public func first() : ?DataT {
            return List.get<DataT>(_items, 0);
        };

        public func limit(n : Nat) : QuerySet<DataT> {
            let limited_items = List.take<DataT>(_items, n);
            return QuerySet<DataT>(?List.toArray<DataT>(limited_items));
        };

        public func orderBy(compare : (DataT, DataT) -> Order.Order) : QuerySet<DataT> {
            let ordered_items = Array.sort<DataT>(List.toArray<DataT>(_items), compare);
            return QuerySet<DataT>(?ordered_items);
        };

        public func count() : Nat {
            return List.size<DataT>(_items);
        };

        public func value() : List.List<DataT> {
            return _items;
        };
    };
};
