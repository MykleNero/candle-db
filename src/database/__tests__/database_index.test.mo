import List "mo:base/List";
import Nat "mo:base/Nat";
import Principal "mo:base/Principal";
import Text "mo:base/Text";

import Test "../../../test";
import TestRunner "../../../test/test_runner";
import DatabaseIndex "../../database/database_index";

module {
    public func run(r : TestRunner.TestRunner, caller : Principal) : async () {
        await r.describe(
            "DatabaseIndex.DatabaseIndex",
            func() : async () {
                await r.describe(
                    "get",
                    func() : async () {
                        await r.it(
                            "should return null when the key is not present",
                            func() : async Test.TestResult {
                                var index = DatabaseIndex.DatabaseIndex<Nat>({
                                    name = "title_idx";
                                });
                                var val = index.get("Jurrasic Park");

                                switch (val) {
                                    case null { #ok };
                                    case (?val) {
                                        #err(#failedTest(?("Expected null but got " # debug_show (val))));
                                    };
                                };
                            },
                        );

                        await r.it(
                            "should return the right value when the key is present",
                            func() : async Test.TestResult {
                                var index = DatabaseIndex.DatabaseIndex<Nat>({
                                    name = "title_idx";
                                });
                                var movie_id = 42;
                                var movie_title = "Jurrasic Park";
                                index.put(movie_title, movie_id);

                                var val = index.get(movie_title);

                                switch (val) {
                                    case null {
                                        #err(#failedTest(?("Received null but expected a list containing: " # Nat.toText(movie_id))));
                                    };
                                    case (?val) {
                                        let found = List.some<Nat>(
                                            ?val,
                                            func id { id == movie_id },
                                        );
                                        if (found) { return #ok };
                                        return #err(#failedTest(?("Expected a list containing " # Nat.toText(movie_id) # " but got " # debug_show (val))));
                                    };
                                };
                            },
                        );
                    },
                );

                await r.describe(
                    "put",
                    func() : async () {
                        await r.it(
                            "should not fail",
                            func() : async Test.TestResult {
                                var index = DatabaseIndex.DatabaseIndex<Nat>({
                                    name = "title_idx";
                                });
                                var movie_id = 42;
                                var movie_title = "Jurrasic Park";
                                index.put(movie_title, movie_id);

                                #ok;
                            },
                        );

                        await r.it(
                            "should not fail when the key is already present",
                            func() : async Test.TestResult {
                                var index = DatabaseIndex.DatabaseIndex<Nat>({
                                    name = "title_idx";
                                });
                                var movie_id = 42;
                                var movie_title = "Jurrasic Park";
                                index.put(movie_title, movie_id);
                                index.put(movie_title, movie_id);

                                #ok;
                            },
                        );

                        await r.it(
                            "should add another value when the key is already present and the value is different",
                            func() : async Test.TestResult {
                                var index = DatabaseIndex.DatabaseIndex<Nat>({
                                    name = "title_idx";
                                });
                                var movie_id = 42;
                                var movie_title = "Jurrasic Park";
                                index.put(movie_title, movie_id);
                                index.put(movie_title, movie_id + 1);
                                index.put(movie_title, movie_id + 2);

                                var ids = index.get(movie_title);
                                var expected_ids = List.fromArray([movie_id + 2, movie_id + 1, movie_id]);

                                if (List.equal<Nat>(ids, expected_ids, Nat.equal)) {
                                    #ok;
                                } else {
                                    #err(#failedTest(?("Expected " # debug_show (expected_ids) # " but got " # debug_show (ids))));
                                };
                            },
                        );
                    },
                );
            },
        );
    };
};
