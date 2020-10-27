(ns com.verybigthings.penkala.util.parse-key-test
  (:require [clojure.test :refer :all]
            [com.verybigthings.penkala.util.parse-key :as p]
            [com.verybigthings.penkala.statement.operations :as o]))

(def source
  {:db/schema "public"
   :relation/name "mytable"})

(def db
  {:db/schema "public"})

(def join-source
  {:db/schema "public"
   :db.schema/current "public"
   :relation/name "mytable"
   :relation/joins {:jointable1 {:relation/name "jointable1"
                                 :relation/alias "jointable1"
                                 :db.schema/current "public"}
                    "myschema.jointable2" {:db/schema "myschema"
                                           :db.schema/current "public"
                                           :relation/name "jointable2"
                                           :relation/alias "jointable2"}
                    :jt2alias {:db/schema "myschema"
                               :db.schema/current "public"
                               :relation/name "jointable2"
                               :relation/alias "jt2alias"}}})

(deftest field-identifiers
  (let [result (p/parse-key source "myfield")]
    (is (= {:db/schema "public" :relation/name "mytable" :query/field "myfield"}
          (select-keys result [:relation/name :db/schema :query/field])))
    (is (= "\"myfield\"" (p/get-path result source db)))
    (is (= "\"myfield\"" (p/get-lhs result source db)))))

(deftest it-should-not-double-quote-a-quoted-field-identifier
  (let [result (p/parse-key source "\"my field\"")]
    (is (= {:db/schema "public" :relation/name "mytable" :query/field "my field"}
          (select-keys result [:relation/name :db/schema :query/field])))
    (is (= "\"my field\"" (p/get-path result source db)))
    (is (= "\"my field\"" (p/get-lhs result source db)))))

(deftest it-should-format-a-shallow-json-path
  (let [result (p/parse-key source "json.property")]
    (is (= {:db/schema "public" :relation/name "mytable" :query/field "json"}
          (select-keys result [:relation/name :db/schema :query/field])))
    (is (= "\"json\"" (p/get-path result source db)))
    (is (= "\"json\"->>'property'" (p/get-lhs result source db)))))

(deftest it-should-format-a-shallow-json-path-with-numeric-key
  (let [result (p/parse-key source "json.123")]
    (is (= {:db/schema "public" :relation/name "mytable" :query/field "json"}
          (select-keys result [:relation/name :db/schema :query/field])))
    (is (= "\"json\"" (p/get-path result source db)))
    (is (= "\"json\"->>'123'" (p/get-lhs result source db)))))

(deftest it-should-format-a-json-array-path
  (let [result (p/parse-key source "json[123]")]
    (is (= {:db/schema "public" :relation/name "mytable" :query/field "json"}
          (select-keys result [:relation/name :db/schema :query/field])))
    (is (= "\"json\"" (p/get-path result source db)))
    (is (= "\"json\"->>123" (p/get-lhs result source db)))))

(deftest it-should-format-a-deep-json-path
  (let [result (p/parse-key source "json.outer.inner]")]
    (is (= {:db/schema "public" :relation/name "mytable" :query/field "json"}
          (select-keys result [:relation/name :db/schema :query/field])))
    (is (= "\"json\"" (p/get-path result source db)))
    (is (= "\"json\"#>>'{outer,inner}'" (p/get-lhs result source db)))))

(deftest it-should-format-a-json-path-with-a-quoted-field
  (let [result (p/parse-key source "\"json field\".outer.inner]")]
    (is (= {:db/schema "public" :relation/name "mytable" :query/field "json field"}
          (select-keys result [:relation/name :db/schema :query/field])))
    (is (= "\"json field\"" (p/get-path result source db)))
    (is (= "\"json field\"#>>'{outer,inner}'" (p/get-lhs result source db)))))

(deftest it-should-format-a-json-path-with-a-quoted-field-containing-special-characters
  (let [result (p/parse-key source "\"json fiel[d]\".outer.inner]")]
    (is (= {:db/schema "public" :relation/name "mytable" :query/field "json fiel[d]"}
          (select-keys result [:relation/name :db/schema :query/field])))
    (is (= "\"json fiel[d]\"" (p/get-path result source db)))
    (is (= "\"json fiel[d]\"#>>'{outer,inner}'" (p/get-lhs result source db)))))

(deftest it-should-format-a-deep-json-path-with-numeric-keys
  (let [result (p/parse-key source "json.123.456]")]
    (is (= {:db/schema "public" :relation/name "mytable" :query/field "json"}
          (select-keys result [:relation/name :db/schema :query/field])))
    (is (= "\"json\"" (p/get-path result source db)))
    (is (= "\"json\"#>>'{123,456}'" (p/get-lhs result source db)))))

(deftest it-should-format-mixed-json-paths
  (let [result (p/parse-key source "json.array[1].field.array[2]]")]
    (is (= {:db/schema "public" :relation/name "mytable" :query/field "json"}
          (select-keys result [:relation/name :db/schema :query/field])))
    (is (= "\"json\"" (p/get-path result source db)))
    (is (= "\"json\"#>>'{array,1,field,array,2}'" (p/get-lhs result source db)))))

(deftest it-should-format-a-shallow-json-path-with-as-text-off
  (let [result (p/parse-key source "json.property" false)]
    (is (= {:db/schema "public" :relation/name "mytable" :query/field "json"}
          (select-keys result [:relation/name :db/schema :query/field])))
    (is (= "\"json\"" (p/get-path result source db)))
    (is (= "\"json\"->'property'" (p/get-lhs result source db)))))

(deftest it-should-format-a-json-array-path-with-as-text-off
  (let [result (p/parse-key source "json[123]" false)]
    (is (= {:db/schema "public" :relation/name "mytable" :query/field "json"}
          (select-keys result [:relation/name :db/schema :query/field])))
    (is (= "\"json\"" (p/get-path result source db)))
    (is (= "\"json\"->123" (p/get-lhs result source db)))))

(deftest it-should-format-a-deep-json-path-with-as-text-off
  (let [result (p/parse-key source "json.outer.inner" false)]
    (is (= {:db/schema "public" :relation/name "mytable" :query/field "json"}
          (select-keys result [:relation/name :db/schema :query/field])))
    (is (= "\"json\"" (p/get-path result source db)))
    (is (= "\"json\"#>'{outer,inner}'" (p/get-lhs result source db)))))

(deftest it-should-force-as-text-on-if-json-has-cast
  (let [result (p/parse-key source "json.property::int" false)]
    (is (= {:db/schema "public" :relation/name "mytable" :query/field "json"}
          (select-keys result [:relation/name :db/schema :query/field])))
    (is (= "\"json\"" (p/get-path result source db)))
    (is (= "(\"json\"->>'property')::int" (p/get-lhs result source db)))))

(deftest it-should-cast-fields-without-an-operator
  (let [result (p/parse-key source "field::text")]
    (is (= {:db/schema "public" :relation/name "mytable" :query/field "field"}
          (select-keys result [:relation/name :db/schema :query/field])))
    (is (= "\"field\"" (p/get-path result source db)))
    (is (= "\"field\"::text" (p/get-lhs result source db)))))

(deftest it-should-quote-an-unquoted-field-identifier-for-a-table
  (let [result (p/parse-key join-source "jointable1.myfield")]
    (is (= "jointable1" (:relation/name result)))
    (is (= "myfield" (:query/field result)))
    (is (= "\"jointable1\".\"myfield\"" (p/get-path result source db)))
    (is (= "\"jointable1\".\"myfield\"" (p/get-lhs result source db)))))

(deftest it-should-quote-an-unquoted-field-identifier-with-the-origin-table-and-schema
  (let [join-source-1 {:db/schema "myschema"
                       :db.schema/current "public"
                       :relation/name "jointable2"
                       :relation/joins {:mytable {:db/schema "public"
                                                  :db.schema/current "public"
                                                  :relation/name "mytable"}}}
        result        (p/parse-key join-source-1 "myschema.jointable2.myfield")]
    (is (= "myschema" (:db/schema result)))
    (is (= "jointable2" (:relation/name result)))
    (is (= "myfield" (:query/field result)))
    (is (= "\"myschema\".\"jointable2\".\"myfield\"" (p/get-path result join-source-1 db)))
    (is (= "\"myschema\".\"jointable2\".\"myfield\"" (p/get-lhs result join-source-1 db)))))

(deftest it-should-quote-an-unquoted-field-identifier-with-an-alias
  (let [result (p/parse-key join-source "jt2alias.myfield")]
    (is (= "jt2alias" (:relation/name result)))
    (is (= "myfield" (:query/field result)))
    (is (= "\"jt2alias\".\"myfield\"" (p/get-path result join-source db)))
    (is (= "\"jt2alias\".\"myfield\"" (p/get-lhs result join-source db)))))

(deftest it-should-quote-an-unquoted-field-identifier-for-a-table-1
  (let [join-source-1 {:db/schema "public"
                       :db.schema/current "public"
                       :relation/name "mytable"
                       :relation/joins {:jt {:relation/name "jointable1"
                                             :db.schema/current "public"
                                             :relation/alias "jt"}}}
        result        (p/parse-key join-source-1 "jointable1.myfield")]
    (is (= "jointable1" (:relation/name result)))
    (is (= "myfield" (:query/field result)))
    (is (= "\"jt\".\"myfield\"" (p/get-path result join-source-1 db)))
    (is (= "\"jt\".\"myfield\"" (p/get-lhs result join-source-1 db)))))

(deftest it-should-default-to-the-origin-for-a-join-source-if-no-schema-or-table-is-specified
  (let [result (p/parse-key join-source "myfield")]
    (is (= "mytable" (:relation/name result)))
    (is (= "myfield" (:query/field result)))
    (is (= "\"mytable\".\"myfield\"" (p/get-path result join-source db)))
    (is (= "\"mytable\".\"myfield\"" (p/get-lhs result join-source db)))))

(deftest it-should-not-double-quote-a-quoted-field-identifier-for-a-table
  (let [join-source-1 {:db/schema "public"
                       :db.schema/current "public"
                       :relation/name "mytable"
                       :relation/joins {:jt {:relation/name "jointable1"
                                             :db.schema/current "public"
                                             :relation/alias "jt"}}}
        result        (p/parse-key join-source-1 "jointable1.\"my field\"")]
    (is (= "jointable1" (:relation/name result)))
    (is (= "my field" (:query/field result)))
    (is (= "\"jt\".\"my field\"" (p/get-path result join-source-1 db)))
    (is (= "\"jt\".\"my field\"" (p/get-lhs result join-source-1 db)))))

(deftest it-should-alias-a-quoted-field-identifier-for-a-table-and-schema
  (let [result (p/parse-key join-source "\"myschema\".\"jointable2\".\"my field\"")]
    (is (= "myschema" (:db/schema result)))
    (is (= "jointable2" (:relation/name result)))
    (is (= "my field" (:query/field result)))
    (is (= "\"jointable2\".\"my field\"" (p/get-path result join-source db)))
    (is (= "\"jointable2\".\"my field\"" (p/get-lhs result join-source db)))))

(deftest it-should-not-double-quote-mixed-quoting-situations-for-a-table
  (let [result (p/parse-key join-source "jointable1.\"my field\"")]
    (is (= "jointable1" (:relation/name result)))
    (is (= "my field" (:query/field result)))
    (is (= "\"jointable1\".\"my field\"" (p/get-path result join-source db)))
    (is (= "\"jointable1\".\"my field\"" (p/get-lhs result join-source db)))))

(deftest it-should-not-double-quote-mixed-quoting-situations-for-a-table-and-schema
  (let [result (p/parse-key join-source "\"myschema\".jointable2.\"my field\"")]
    (is (= "myschema" (:db/schema result)))
    (is (= "jointable2" (:relation/name result)))
    (is (= "my field" (:query/field result)))
    (is (= "\"jointable2\".\"my field\"" (p/get-path result join-source db)))
    (is (= "\"jointable2\".\"my field\"" (p/get-lhs result join-source db)))))

(deftest it-should-format-mixed-json-paths-with-a-table
  (let [result (p/parse-key join-source "jointable1.json.array[1].field.array[2]")]
    (is (= "jointable1" (:relation/name result)))
    (is (= "json" (:query/field result)))
    (is (= "\"jointable1\".\"json\"" (p/get-path result join-source db)))
    (is (= "\"jointable1\".\"json\"#>>'{array,1,field,array,2}'" (p/get-lhs result join-source db)))))

(deftest it-should-format-mixed-json-paths-with-a-table-and-schema
  (let [result (p/parse-key join-source "myschema.jointable2.json.array[1].field.array[2]")]
    (is (= "myschema" (:db/schema result)))
    (is (= "jointable2" (:relation/name result)))
    (is (= "json" (:query/field result)))
    (is (= "\"jointable2\".\"json\"" (p/get-path result join-source db)))
    (is (= "\"jointable2\".\"json\"#>>'{array,1,field,array,2}'" (p/get-lhs result join-source db)))))

(deftest with-appendix-should-default-to-equivalence
  (let [result (p/with-appendix source "myfield" o/operations)]
    (is (= "myfield" (:query/field result)))
    (is (= "\"myfield\"" (p/get-path result source db)))
    (is (= "\"myfield\"" (p/get-lhs result source db)))
    (is (= "=" (get-in result [:appended :operator])))))

(deftest with-appendix-should-get-operation-details-for-an-unquoted-identifier
  (let [result (p/with-appendix source "myfield <=" o/operations)]
    (is (= (:query/field result) "myfield"))
    (is (= "\"myfield\"" (p/get-path result source db)))
    (is (= "\"myfield\"" (p/get-lhs result source db)))
    (is (= "<=" (get-in result [:appended :operator])))))

(deftest with-appendix-should-get-operation-details-for-a-quoted-identifier
  (let [result (p/with-appendix source "\"my field\" <=" o/operations)]
    (is (= (:query/field result) "my field"))
    (is (= "\"my field\"" (p/get-path result source db)))
    (is (= "\"my field\"" (p/get-lhs result source db)))
    (is (= "<=" (get-in result [:appended :operator])))))

(deftest with-appendix-should-get-an-operation-comprising-multiple-tokens
  (let [result (p/with-appendix source "myfield not similar to" o/operations)]
    (is (= (:query/field result) "myfield"))
    (is (= "\"myfield\"" (p/get-path result source db)))
    (is (= "\"myfield\"" (p/get-lhs result source db)))
    (is (= "NOT SIMILAR TO" (get-in result [:appended :operator])))))

(deftest with-appendix-should-allow-any-amount-of-whitespace
  (let [result (p/with-appendix source "   \r\n \t  myfield \r\n \t \n \t <= \r" o/operations)]
    (is (= (:query/field result) "myfield"))
    (is (= "\"myfield\"" (p/get-path result source db)))
    (is (= "\"myfield\"" (p/get-lhs result source db)))
    (is (= "<=" (get-in result [:appended :operator])))))

(deftest with-appendix-should-get-the-appropriate-mutator
  (let [result (p/with-appendix source "\"my field\" @>" o/operations)]
    (is (= (:query/field result) "my field"))
    (is (= "\"my field\"" (p/get-path result source db)))
    (is (= "\"my field\"" (p/get-lhs result source db)))
    (is (= "@>" (get-in result [:appended :operator])))
    (is (= {:offset 1 :value "?" :params ["{hi}"]}
          ((get-in result [:appended :mutator]) {:value ["hi"] :params [] :offset 1})))))

(deftest with-appendix-should-get-operations-for-a-shallow-json-path
  (let [result (p/with-appendix source "json.key <=" o/operations)]
    (is (= "json" (:query/field result)))
    (is (= "\"json\"" (p/get-path result source db)))
    (is (= "\"json\"->>'key'" (p/get-lhs result source db)))
    (is (= "<=" (get-in result [:appended :operator])))))

(deftest with-appendix-should-get-operations-for-a-deep-json-path
  (let [result (p/with-appendix source "json.outer.inner <=" o/operations)]
    (is (= "json" (:query/field result)))
    (is (= "\"json\"" (p/get-path result source db)))
    (is (= "\"json\"#>>'{outer,inner}'" (p/get-lhs result source db)))
    (is (= "<=" (get-in result [:appended :operator])))))

(deftest with-appendix-should-get-operations-for-a-json-array
  (let [result (p/with-appendix source "json[1] <=" o/operations)]
    (is (= "json" (:query/field result)))
    (is (= "\"json\"" (p/get-path result source db)))
    (is (= "\"json\"->>1" (p/get-lhs result source db)))
    (is (= "<=" (get-in result [:appended :operator])))))

(deftest with-appendix-should-match->-properly
  (let [result (p/with-appendix source "field >" o/operations)]
    (is (= "field" (:query/field result)))
    (is (= "\"field\"" (p/get-path result source db)))
    (is (= "\"field\"" (p/get-lhs result source db)))
    (is (= ">" (get-in result [:appended :operator])))))

(deftest with-appendix-should-match-the-longest-possible-operator
  (let [result (p/with-appendix source "field ~~*" o/operations)]
    (is (= "field" (:query/field result)))
    (is (= "\"field\"" (p/get-path result source db)))
    (is (= "\"field\"" (p/get-lhs result source db)))
    (is (= "ILIKE" (get-in result [:appended :operator])))))

(deftest with-appendix-should-ignore-case-of-LIKE-and-similar-operators
  (let [result (p/with-appendix source "field LikE" o/operations)]
    (is (= "field" (:query/field result)))
    (is (= "\"field\"" (p/get-path result source db)))
    (is (= "\"field\"" (p/get-lhs result source db)))
    (is (= "LIKE" (get-in result [:appended :operator])))))

(deftest with-appendix-should-not-clobber-a-field-with-an-operator-in-the-name
  (let [result (p/with-appendix source "is_field is" o/operations)]
    (is (= "is_field" (:query/field result)))
    (is (= "\"is_field\"" (p/get-path result source db)))
    (is (= "\"is_field\"" (p/get-lhs result source db)))
    (is (= "IS" (get-in result [:appended :operator])))))

(deftest with-appendix-should-not-clobber-a-quoted-field-with-an-operator-in-the-name
  (let [result (p/with-appendix source "\"this is a field\" is" o/operations)]
    (is (= "this is a field" (:query/field result)))
    (is (= "\"this is a field\"" (p/get-path result source db)))
    (is (= "\"this is a field\"" (p/get-lhs result source db)))
    (is (= "IS" (get-in result [:appended :operator])))))

(deftest with-appendix-should-get-the-operation-details-for-an-identifier-with-table
  (let [result (p/with-appendix join-source "\"jointable1\".\"my field\" <=" o/operations)]
    (is (= "jointable1" (:relation/name result)))
    (is (= "my field" (:query/field result)))
    (is (= "\"jointable1\".\"my field\"" (p/get-path result join-source db)))
    (is (= "\"jointable1\".\"my field\"" (p/get-lhs result join-source db)))
    (is (= "<=" (get-in result [:appended :operator])))))

(deftest with-appendix-should-get-the-operation-details-for-an-identifier-with-table-and-schema
  (let [result (p/with-appendix join-source "myschema.jointable2.myfield <=" o/operations)]
    (is (= "myschema" (:db/schema result)))
    (is (= "jointable2" (:relation/name result)))
    (is (= "myfield" (:query/field result)))
    (is (= "\"jointable2\".\"myfield\"" (p/get-path result join-source db)))
    (is (= "\"jointable2\".\"myfield\"" (p/get-lhs result join-source db)))
    (is (= "<=" (get-in result [:appended :operator])))))

(deftest with-appendix-should-get-operations-for-a-deep-json-path-with-table-and-schema
  (let [result (p/with-appendix join-source "myschema.jointable2.json.outer.inner <=" o/operations)]
    (is (= "myschema" (:db/schema result)))
    (is (= "jointable2" (:relation/name result)))
    (is (= "json" (:query/field result)))
    (is (= "\"jointable2\".\"json\"" (p/get-path result join-source db)))
    (is (= "\"jointable2\".\"json\"#>>'{outer,inner}'" (p/get-lhs result join-source db)))
    (is (= "<=" (get-in result [:appended :operator])))))

(deftest with-appendix-should-format-mixed-json-paths
  (let [result (p/with-appendix source "json.array[1].field.array[2]::boolean LIKE" o/operations)]
    (is (= "json" (:query/field result)))
    (is (= "\"json\"" (p/get-path result source db)))
    (is (= "(\"json\"#>>'{array,1,field,array,2}')::boolean" (p/get-lhs result source db)))
    (is (= "LIKE" (get-in result [:appended :operator])))))

(deftest with-appendix-should-format-quoted-fields-with-mixed-json-paths
  (let [result (p/with-appendix source "\"json\".array[1].field.array[2]::boolean LIKE" o/operations)]
    (is (= "json" (:query/field result)))
    (is (= "\"json\"" (p/get-path result source db)))
    (is (= "(\"json\"#>>'{array,1,field,array,2}')::boolean" (p/get-lhs result source db)))
    (is (= "LIKE" (get-in result [:appended :operator])))))

(deftest with-appendix-should-cast-fields
  (let [result (p/with-appendix source "field::text LIKE" o/operations)]
    (is (= "field" (:query/field result)))
    (is (= "\"field\"" (p/get-path result source db)))
    (is (= "\"field\"::text" (p/get-lhs result source db)))
    (is (= "LIKE" (get-in result [:appended :operator])))))

(deftest with-appendix-should-cast-fields-with-shallow-json-paths
  (let [result (p/with-appendix source "field.element::boolean LIKE" o/operations)]
    (is (= "field" (:query/field result)))
    (is (= "\"field\"" (p/get-path result source db)))
    (is (= "(\"field\"->>'element')::boolean" (p/get-lhs result source db)))
    (is (= "LIKE" (get-in result [:appended :operator])))))

(deftest with-appendix-should-cast-fields-with-shallow-json-paths
  (let [result (p/with-appendix source "field.one.two::boolean LIKE" o/operations)]
    (is (= "field" (:query/field result)))
    (is (= "\"field\"" (p/get-path result source db)))
    (is (= "(\"field\"#>>'{one,two}')::boolean" (p/get-lhs result source db)))
    (is (= "LIKE" (get-in result [:appended :operator])))))

(deftest with-appendix-should-cast-fields-with-json-arrays
  (let [result (p/with-appendix source "field[123]::boolean LIKE" o/operations)]
    (is (= "field" (:query/field result)))
    (is (= "\"field\"" (p/get-path result source db)))
    (is (= "(\"field\"->>123)::boolean" (p/get-lhs result source db)))
    (is (= "LIKE" (get-in result [:appended :operator])))))

(deftest with-appendix-should-cast-quoted-fields-without-an-operator
  (let [result (p/with-appendix source "\"field\"::text" o/operations)]
    (is (= "field" (:query/field result)))
    (is (= "\"field\"" (p/get-path result source db)))
    (is (= "\"field\"::text" (p/get-lhs result source db)))
    (is (= "=" (get-in result [:appended :operator])))))

(deftest with-appendix-should-cast-quoted-fields
  (let [result (p/with-appendix source "\"field\"::text LIKE" o/operations)]
    (is (= "field" (:query/field result)))
    (is (= "\"field\"" (p/get-path result source db)))
    (is (= "\"field\"::text" (p/get-lhs result source db)))
    (is (= "LIKE" (get-in result [:appended :operator])))))

(deftest with-appendix-should-cast-quoted-fields-with-json-operations
  (let [result (p/with-appendix source "\"field\".element::boolean LIKE" o/operations)]
    (is (= "field" (:query/field result)))
    (is (= "\"field\"" (p/get-path result source db)))
    (is (= "(\"field\"->>'element')::boolean" (p/get-lhs result source db)))
    (is (= "LIKE" (get-in result [:appended :operator])))))

(deftest with-appendix-should-cast-quoted-fields-with-deep-json-paths
  (let [result (p/with-appendix source "\"field\".one.two::boolean LIKE" o/operations)]
    (is (= "field" (:query/field result)))
    (is (= "\"field\"" (p/get-path result source db)))
    (is (= "(\"field\"#>>'{one,two}')::boolean" (p/get-lhs result source db)))
    (is (= "LIKE" (get-in result [:appended :operator])))))

(deftest with-appendix-should-cast-quoted-fields-with-json-arrays
  (let [result (p/with-appendix source "\"field\"[123]::boolean LIKE" o/operations)]
    (is (= "field" (:query/field result)))
    (is (= "\"field\"" (p/get-path result source db)))
    (is (= "(\"field\"->>123)::boolean" (p/get-lhs result source db)))
    (is (= "LIKE" (get-in result [:appended :operator])))))

(deftest with-appendix-should-cast-fields-with-a-table
  (let [result (p/with-appendix join-source "jointable1.field::text LIKE" o/operations)]
    (is (= "jointable1" (:relation/name result)))
    (is (= "field" (:query/field result)))
    (is (= "\"jointable1\".\"field\"" (p/get-path result join-source db)))
    (is (= "\"jointable1\".\"field\"::text" (p/get-lhs result join-source db)))
    (is (= "LIKE" (get-in result [:appended :operator])))))

(deftest with-appendix-should-cast-fields-with-a-schema-and-table
  (let [result (p/with-appendix join-source "myschema.jointable2.field::text LIKE" o/operations)]
    (is (= "myschema" (:db/schema result)))
    (is (= "jointable2" (:relation/name result)))
    (is (= "field" (:query/field result)))
    (is (= "\"jointable2\".\"field\"" (p/get-path result join-source db)))
    (is (= "\"jointable2\".\"field\"::text" (p/get-lhs result join-source db)))
    (is (= "LIKE" (get-in result [:appended :operator])))))

(deftest with-appendix-should-cast-mixed-json-paths-with-a-schema-and-table
  (let [result (p/with-appendix join-source "myschema.jointable2.json.array[1].field.array[2]::boolean LIKE" o/operations)]
    (is (= "myschema" (:db/schema result)))
    (is (= "jointable2" (:relation/name result)))
    (is (= "json" (:query/field result)))
    (is (= "\"jointable2\".\"json\"" (p/get-path result join-source db)))
    (is (= "(\"jointable2\".\"json\"#>>'{array,1,field,array,2}')::boolean" (p/get-lhs result join-source db)))
    (is (= "LIKE" (get-in result [:appended :operator])))))

