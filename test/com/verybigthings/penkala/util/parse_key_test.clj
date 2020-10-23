(ns com.verybigthings.penkala.util.parse-key-test
  (:require [clojure.test :refer :all]
            [com.verybigthings.penkala.util.parse-key :as p]
            [com.verybigthings.penkala.statement.operations :as o]))

(def source
  {:schema "public"
   :name "mytable"})

(def db
  {:schema "public"})

(def join-source
  {:schema "public"
   :name "mytable"
   :joins {:jointable1 {:relation "jointable1"
                        :alias "jointable1"}
           "myschema.jointable2" {:schema "myschema"
                                  :relation "jointable2"
                                  :alias "jointable2"}
           :jt2alias {:schema "myschema"
                      :relation "jointable2"
                      :alias "jt2alias"}}})

(deftest field-identifiers
  (let [result (p/parse-key db source "myfield")]
    (is (= "myfield" (:field result)))
    (is (= "\"myfield\"" (:path result)))
    (is (= "\"myfield\"" (:lhs result)))
    (is (= "mytable" (:relation result)))
    (is (nil? (:schema result)))))

(deftest it-should-not-double-quote-a-quoted-field-identifier
  (let [result (p/parse-key db source "\"my field\"")]
    (is (= "my field" (:field result)))
    (is (= "\"my field\"" (:path result)))
    (is (= "\"my field\"" (:lhs result)))
    (is (= "mytable" (:relation result)))
    (is (nil? (:schema result)))))

(deftest it-should-format-a-shallow-json-path
  (let [result (p/parse-key db source "json.property")]
    (is (= "json" (:field result)))
    (is (= "\"json\"" (:path result)))
    (is (= "\"json\"->>'property'" (:lhs result)))
    (is (= ["property"] (:json-elements result)))))

(deftest it-should-format-a-shallow-json-path-with-numeric-key
  (let [result (p/parse-key db source "json.123")]
    (is (= "json" (:field result)))
    (is (= "\"json\"" (:path result)))
    (is (= "\"json\"->>'123'" (:lhs result)))
    (is (= ["123"] (:json-elements result)))))

(deftest it-should-format-a-json-array-path
  (let [result (p/parse-key db source "json[123]")]
    (is (= "json" (:field result)))
    (is (= "\"json\"" (:path result)))
    (is (= "\"json\"->>123" (:lhs result)))
    (is (= ["123"] (:json-elements result)))))

(deftest it-should-format-a-deep-json-path
  (let [result (p/parse-key db source "json.outer.inner")]
    (is (= "json" (:field result)))
    (is (= "\"json\"" (:path result)))
    (is (= "\"json\"#>>'{outer,inner}'" (:lhs result)))
    (is (= ["outer" "inner"] (:json-elements result)))))

(deftest it-should-format-a-json-path-with-a-quoted-field
  (let [result (p/parse-key db source "\"json field\".outer.inner")]
    (is (= "json field" (:field result)))
    (is (= "\"json field\"" (:path result)))
    (is (= "\"json field\"#>>'{outer,inner}'" (:lhs result)))
    (is (= ["outer" "inner"] (:json-elements result)))))

(deftest it-should-format-a-json-path-with-a-quoted-field-containing-special-characters
  (let [result (p/parse-key db source "\"json fiel[d]\".outer.inner")]
    (is (= "json fiel[d]" (:field result)))
    (is (= "\"json fiel[d]\"" (:path result)))
    (is (= "\"json fiel[d]\"#>>'{outer,inner}'" (:lhs result)))
    (is (= ["outer" "inner"] (:json-elements result)))))

(deftest it-should-format-a-deep-json-path-with-numeric-keys
  (let [result (p/parse-key db source "json.123.456")]
    (is (= "json" (:field result)))
    (is (= "\"json\"" (:path result)))
    (is (= "\"json\"#>>'{123,456}'" (:lhs result)))
    (is (= ["123" "456"] (:json-elements result)))))

(deftest it-should-format-mixed-json-paths
  (let [result (p/parse-key db source "json.array[1].field.array[2]")]
    (is (= "json" (:field result)))
    (is (= "\"json\"" (:path result)))
    (is (= "\"json\"#>>'{array,1,field,array,2}'" (:lhs result)))
    (is (= ["array" "1" "field" "array" "2"] (:json-elements result)))))

(deftest it-should-format-a-shallow-json-path-with-as-text-off
  (let [result (p/parse-key db source "json.property" false)]
    (is (= "json" (:field result)))
    (is (= "\"json\"" (:path result)))
    (is (= "\"json\"->'property'" (:lhs result)))
    (is (= ["property"] (:json-elements result)))))

(deftest it-should-format-a-json-array-path-with-as-text-off
  (let [result (p/parse-key db source "json[123]" false)]
    (is (= "json" (:field result)))
    (is (= "\"json\"" (:path result)))
    (is (= "\"json\"->123" (:lhs result)))
    (is (= ["123"] (:json-elements result)))))

(deftest it-should-format-a-deep-json-path-with-as-text-off
  (let [result (p/parse-key db source "json.outer.inner" false)]
    (is (= "json" (:field result)))
    (is (= "\"json\"" (:path result)))
    (is (= "\"json\"#>'{outer,inner}'" (:lhs result)))
    (is (= ["outer" "inner"] (:json-elements result)))))

(deftest it-should-force-as-text-on-if-json-has-cast
  (let [result (p/parse-key db source "json.property::int" false)]
    (is (= "json" (:field result)))
    (is (= "\"json\"" (:path result)))
    (is (= "(\"json\"->>'property')::int" (:lhs result)))
    (is (= ["property"] (:json-elements result)))))

(deftest it-should-cast-fields-without-an-operator
  (let [result (p/parse-key db source "field::text")]
    (is (= "field" (:field result)))
    (is (= "\"field\"" (:path result)))
    (is (= "\"field\"::text" (:lhs result)))
    (is (nil? (:appended result)))))

(deftest it-should-quote-an-unquoted-field-identifier-for-a-table
  (let [result (p/parse-key db join-source "jointable1.myfield")]
    (is (= "jointable1" (:relation result)))
    (is (= "myfield" (:field result)))
    (is (= "\"jointable1\".\"myfield\"" (:path result)))
    (is (= "\"jointable1\".\"myfield\"" (:lhs result)))))

(deftest it-should-quote-an-unquoted-field-identifier-with-the-origin-table-and-schema
  (let [join-source-1 {:schema "myschema"
                       :name "jointable2"
                       :joins {:mytable {:schema "public"
                                         :relation "mytable"}}}
        result (p/parse-key db join-source-1 "myschema.jointable2.myfield")]
    (is (= "myschema" (:schema result)))
    (is (= "jointable2" (:relation result)))
    (is (= "myfield" (:field result)))
    (is (= "\"myschema\".\"jointable2\".\"myfield\"" (:path result)))
    (is (= "\"myschema\".\"jointable2\".\"myfield\"" (:lhs result)))))

(deftest it-should-quote-an-unquoted-field-identifier-with-an-alias
  (let [result (p/parse-key db join-source "jt2alias.myfield")]
    (is (nil? (:schema result)))
    (is (= "jt2alias" (:relation result)))
    (is (= "myfield" (:field result)))
    (is (= "\"jt2alias\".\"myfield\"" (:path result)))
    (is (= "\"jt2alias\".\"myfield\"" (:lhs result)))))

(deftest it-should-quote-an-unquoted-field-identifier-for-a-table-1
  (let [join-source-1 {:schema "public"
                       :name "mytable"
                       :joins {:jt {:relation "jointable1"
                                    :alias "jt"}}}
        result (p/parse-key db join-source-1 "jointable1.myfield")]
    (is (= "jointable1" (:relation result)))
    (is (= "myfield" (:field result)))
    (is (= "\"jt\".\"myfield\"" (:path result)))
    (is (= "\"jt\".\"myfield\"" (:lhs result)))))

(deftest it-should-default-to-the-origin-for-a-join-source-if-no-schema-or-table-is-specified
  (let [result (p/parse-key db join-source "myfield")]
    (is (= "public" (:schema result)))
    (is (= "mytable" (:relation result)))
    (is (= "myfield" (:field result)))
    (is (= "\"mytable\".\"myfield\"" (:path result)))
    (is (= "\"mytable\".\"myfield\"" (:lhs result)))))

(deftest it-should-not-double-quote-a-quoted-field-identifier-for-a-table
  (let [join-source-1 {:schema "public"
                       :name "mytable"
                       :joins {:jt {:relation "jointable1"
                                    :alias "jt"}}}
        result (p/parse-key db join-source-1 "jointable1.\"my field\"")]
    (is (= "jointable1" (:relation result)))
    (is (= "my field" (:field result)))
    (is (= "\"jt\".\"my field\"" (:path result)))
    (is (= "\"jt\".\"my field\"" (:lhs result)))))

(deftest it-should-alias-a-quoted-field-identifier-for-a-table-and-schema
  (let [result (p/parse-key db join-source "\"myschema\".\"jointable2\".\"my field\"")]
    (is (= "myschema" (:schema result)))
    (is (= "jointable2" (:relation result)))
    (is (= "my field" (:field result)))
    (is (= "\"jointable2\".\"my field\"" (:path result)))
    (is (= "\"jointable2\".\"my field\"" (:lhs result)))))

(deftest it-should-not-double-quote-mixed-quoting-situations-for-a-table
  (let [result (p/parse-key db join-source "jointable1.\"my field\"")]
    (is (= "jointable1" (:relation result)))
    (is (= "my field" (:field result)))
    (is (= "\"jointable1\".\"my field\"" (:path result)))
    (is (= "\"jointable1\".\"my field\"" (:lhs result)))))

(deftest it-should-not-double-quote-mixed-quoting-situations-for-a-table-and-schema
  (let [result (p/parse-key db join-source "\"myschema\".jointable2.\"my field\"")]
    (is (= "myschema" (:schema result)))
    (is (= "jointable2" (:relation result)))
    (is (= "my field" (:field result)))
    (is (= "\"jointable2\".\"my field\"" (:path result)))
    (is (= "\"jointable2\".\"my field\"" (:lhs result)))))

(deftest it-should-format-mixed-json-paths-with-a-table
  (let [result (p/parse-key db join-source "jointable1.json.array[1].field.array[2]")]
    (is (= "jointable1" (:relation result)))
    (is (= "json" (:field result)))
    (is (= "\"jointable1\".\"json\"" (:path result)))
    (is (= "\"jointable1\".\"json\"#>>'{array,1,field,array,2}'" (:lhs result)))
    (is (= ["array" "1" "field" "array" "2"] (:json-elements result)))))

(deftest it-should-format-mixed-json-paths-with-a-table-and-schema
  (let [result (p/parse-key db join-source "myschema.jointable2.json.array[1].field.array[2]")]
    (is (= "myschema" (:schema result)))
    (is (= "jointable2" (:relation result)))
    (is (= "json" (:field result)))
    (is (= "\"jointable2\".\"json\"" (:path result)))
    (is (= "\"jointable2\".\"json\"#>>'{array,1,field,array,2}'" (:lhs result)))
    (is (= ["array" "1" "field" "array" "2"] (:json-elements result)))))

(deftest with-appendix-should-default-to-equivalence
  (let [result (p/with-appendix db source "myfield" o/operations)]
    (is (= "myfield" (:field result)))
    (is (= "\"myfield\"" (:path result)))
    (is (= "\"myfield\"" (:lhs result)))
    (is (= "=" (get-in result [:appended :operator])))))

(deftest with-appendix-should-get-operation-details-for-an-unquoted-identifier
  (let [result (p/with-appendix db source "myfield <=" o/operations)]
    (is (= (:field result) "myfield"))
    (is (= "\"myfield\"" (:path result)))
    (is (= "\"myfield\"" (:lhs result)))
    (is (= "<=" (get-in result [:appended :operator])))))

(deftest with-appendix-should-get-operation-details-for-a-quoted-identifier
  (let [result (p/with-appendix db source "\"my field\" <=" o/operations)]
    (is (= (:field result) "my field"))
    (is (= "\"my field\"" (:path result)))
    (is (= "\"my field\"" (:lhs result)))
    (is (= "<=" (get-in result [:appended :operator])))))

(deftest with-appendix-should-get-an-operation-comprising-multiple-tokens
  (let [result (p/with-appendix db source "myfield not similar to" o/operations)]
    (is (= (:field result) "myfield"))
    (is (= "\"myfield\"" (:path result)))
    (is (= "\"myfield\"" (:lhs result)))
    (is (= "NOT SIMILAR TO" (get-in result [:appended :operator])))))

(deftest with-appendix-should-allow-any-amount-of-whitespace
  (let [result (p/with-appendix db source "   \r\n \t  myfield \r\n \t \n \t <= \r" o/operations)]
    (is (= (:field result) "myfield"))
    (is (= "\"myfield\"" (:path result)))
    (is (= "\"myfield\"" (:lhs result)))
    (is (= "<=" (get-in result [:appended :operator])))))

(deftest with-appendix-should-get-the-appropriate-mutator
  (let [result (p/with-appendix db source "\"my field\" @>" o/operations)]
    (is (= (:field result) "my field"))
    (is (= "\"my field\"" (:path result)))
    (is (= "\"my field\"" (:lhs result)))
    (is (= "@>" (get-in result [:appended :operator])))
    (is (= {:offset 1 :value "$1" :params ["{hi}"]}
          ((get-in result [:appended :mutator]) {:value ["hi"] :params [] :offset 1})))))

(deftest with-appendix-should-get-operations-for-a-shallow-json-path
  (let [result (p/with-appendix db source "json.key <=" o/operations)]
    (is (= "json" (:field result)))
    (is (= "\"json\"" (:path result)))
    (is (= "\"json\"->>'key'" (:lhs result)))
    (is (= "<=" (get-in result [:appended :operator])))))

(deftest with-appendix-should-get-operations-for-a-deep-json-path
  (let [result (p/with-appendix db source "json.outer.inner <=" o/operations)]
    (is (= "json" (:field result)))
    (is (= "\"json\"" (:path result)))
    (is (= "\"json\"#>>'{outer,inner}'" (:lhs result)))
    (is (= "<=" (get-in result [:appended :operator])))))

(deftest with-appendix-should-get-operations-for-a-json-array
  (let [result (p/with-appendix db source "json[1] <=" o/operations)]
    (is (= "json" (:field result)))
    (is (= "\"json\"" (:path result)))
    (is (= "\"json\"->>1" (:lhs result)))
    (is (= "<=" (get-in result [:appended :operator])))))

(deftest with-appendix-should-match->-properly
  (let [result (p/with-appendix db source "field >" o/operations)]
    (is (= "field" (:field result)))
    (is (= "\"field\"" (:path result)))
    (is (= "\"field\"" (:lhs result)))
    (is (= ">" (get-in result [:appended :operator])))))

(deftest with-appendix-should-match-the-longest-possible-operator
  (let [result (p/with-appendix db source "field ~~*" o/operations)]
    (is (= "field" (:field result)))
    (is (= "\"field\"" (:path result)))
    (is (= "\"field\"" (:lhs result)))
    (is (= "ILIKE" (get-in result [:appended :operator])))))

(deftest with-appendix-should-ignore-case-of-LIKE-and-similar-operators
  (let [result (p/with-appendix db source "field LikE" o/operations)]
    (is (= "field" (:field result)))
    (is (= "\"field\"" (:path result)))
    (is (= "\"field\"" (:lhs result)))
    (is (= "LIKE" (get-in result [:appended :operator])))))

(deftest with-appendix-should-not-clobber-a-field-with-an-operator-in-the-name
  (let [result (p/with-appendix db source "is_field is" o/operations)]
    (is (= "is_field" (:field result)))
    (is (= "\"is_field\"" (:path result)))
    (is (= "\"is_field\"" (:lhs result)))
    (is (= "IS" (get-in result [:appended :operator])))))

(deftest with-appendix-should-not-clobber-a-quoted-field-with-an-operator-in-the-name
  (let [result (p/with-appendix db source "\"this is a field\" is" o/operations)]
    (is (= "this is a field" (:field result)))
    (is (= "\"this is a field\"" (:path result)))
    (is (= "\"this is a field\"" (:lhs result)))
    (is (= "IS" (get-in result [:appended :operator])))))

(deftest with-appendix-should-get-the-operation-details-for-an-identifier-with-table
  (let [result (p/with-appendix db join-source "\"jointable1\".\"my field\" <=" o/operations)]
    (is (= "jointable1" (:relation result)))
    (is (= "my field" (:field result)))
    (is (= "\"jointable1\".\"my field\"" (:path result)))
    (is (= "\"jointable1\".\"my field\"" (:lhs result)))
    (is (= "<=" (get-in result [:appended :operator])))))

(deftest with-appendix-should-get-the-operation-details-for-an-identifier-with-table-and-schema
  (let [result (p/with-appendix db join-source "myschema.jointable2.myfield <=" o/operations)]
    (is (= "myschema" (:schema result)))
    (is (= "jointable2" (:relation result)))
    (is (= "myfield" (:field result)))
    (is (= "\"jointable2\".\"myfield\"" (:path result)))
    (is (= "\"jointable2\".\"myfield\"" (:lhs result)))
    (is (= "<=" (get-in result [:appended :operator])))))

(deftest with-appendix-should-get-operations-for-a-deep-json-path-with-table-and-schema
  (let [result (p/with-appendix db join-source "myschema.jointable2.json.outer.inner <=" o/operations)]
    (is (= "myschema" (:schema result)))
    (is (= "jointable2" (:relation result)))
    (is (= "json" (:field result)))
    (is (= "\"jointable2\".\"json\"" (:path result)))
    (is (= "\"jointable2\".\"json\"#>>'{outer,inner}'" (:lhs result)))
    (is (= "<=" (get-in result [:appended :operator])))))

(deftest with-appendix-should-format-mixed-json-paths
  (let [result (p/with-appendix db source "json.array[1].field.array[2]::boolean LIKE" o/operations)]
    (is (= "json" (:field result)))
    (is (= "\"json\"" (:path result)))
    (is (= "(\"json\"#>>'{array,1,field,array,2}')::boolean" (:lhs result)))
    (is (= "LIKE" (get-in result [:appended :operator])))))

(deftest with-appendix-should-format-quoted-fields-with-mixed-json-paths
  (let [result (p/with-appendix db source "\"json\".array[1].field.array[2]::boolean LIKE" o/operations)]
    (is (= "json" (:field result)))
    (is (= "\"json\"" (:path result)))
    (is (= "(\"json\"#>>'{array,1,field,array,2}')::boolean" (:lhs result)))
    (is (= "LIKE" (get-in result [:appended :operator])))))

(deftest with-appendix-should-cast-fields
  (let [result (p/with-appendix db source "field::text LIKE" o/operations)]
    (is (= "field" (:field result)))
    (is (= "\"field\"" (:path result)))
    (is (= "\"field\"::text" (:lhs result)))
    (is (= "LIKE" (get-in result [:appended :operator])))))

(deftest with-appendix-should-cast-fields-with-shallow-json-paths
  (let [result (p/with-appendix db source "field.element::boolean LIKE" o/operations)]
    (is (= "field" (:field result)))
    (is (= "\"field\"" (:path result)))
    (is (= "(\"field\"->>'element')::boolean" (:lhs result)))
    (is (= "LIKE" (get-in result [:appended :operator])))))

(deftest with-appendix-should-cast-fields-with-shallow-json-paths
  (let [result (p/with-appendix db source "field.one.two::boolean LIKE" o/operations)]
    (is (= "field" (:field result)))
    (is (= "\"field\"" (:path result)))
    (is (= "(\"field\"#>>'{one,two}')::boolean" (:lhs result)))
    (is (= "LIKE" (get-in result [:appended :operator])))))

(deftest with-appendix-should-cast-fields-with-json-arrays
  (let [result (p/with-appendix db source "field[123]::boolean LIKE" o/operations)]
    (is (= "field" (:field result)))
    (is (= "\"field\"" (:path result)))
    (is (= "(\"field\"->>123)::boolean" (:lhs result)))
    (is (= "LIKE" (get-in result [:appended :operator])))))

(deftest with-appendix-should-cast-quoted-fields-without-an-operator
  (let [result (p/with-appendix db source "\"field\"::text" o/operations)]
    (is (= "field" (:field result)))
    (is (= "\"field\"" (:path result)))
    (is (= "\"field\"::text" (:lhs result)))
    (is (= "=" (get-in result [:appended :operator])))))

(deftest with-appendix-should-cast-quoted-fields
  (let [result (p/with-appendix db source "\"field\"::text LIKE" o/operations)]
    (is (= "field" (:field result)))
    (is (= "\"field\"" (:path result)))
    (is (= "\"field\"::text" (:lhs result)))
    (is (= "LIKE" (get-in result [:appended :operator])))))

(deftest with-appendix-should-cast-quoted-fields-with-json-operations
  (let [result (p/with-appendix db source "\"field\".element::boolean LIKE" o/operations)]
    (is (= "field" (:field result)))
    (is (= "\"field\"" (:path result)))
    (is (= "(\"field\"->>'element')::boolean" (:lhs result)))
    (is (= "LIKE" (get-in result [:appended :operator])))))

(deftest with-appendix-should-cast-quoted-fields-with-deep-json-paths
  (let [result (p/with-appendix db source "\"field\".one.two::boolean LIKE" o/operations)]
    (is (= "field" (:field result)))
    (is (= "\"field\"" (:path result)))
    (is (= "(\"field\"#>>'{one,two}')::boolean" (:lhs result)))
    (is (= "LIKE" (get-in result [:appended :operator])))))

(deftest with-appendix-should-cast-quoted-fields-with-json-arrays
  (let [result (p/with-appendix db source "\"field\"[123]::boolean LIKE" o/operations)]
    (is (= "field" (:field result)))
    (is (= "\"field\"" (:path result)))
    (is (= "(\"field\"->>123)::boolean" (:lhs result)))
    (is (= "LIKE" (get-in result [:appended :operator])))))

(deftest with-appendix-should-cast-fields-with-a-table
  (let [result (p/with-appendix db join-source "jointable1.field::text LIKE" o/operations)]
    (is (= "jointable1" (:relation result)))
    (is (= "field" (:field result)))
    (is (= "\"jointable1\".\"field\"" (:path result)))
    (is (= "\"jointable1\".\"field\"::text" (:lhs result)))
    (is (= "LIKE" (get-in result [:appended :operator])))))

(deftest with-appendix-should-cast-fields-with-a-schema-and-table
  (let [result (p/with-appendix db join-source "myschema.jointable2.field::text LIKE" o/operations)]
    (is (= "myschema" (:schema result)))
    (is (= "jointable2" (:relation result)))
    (is (= "field" (:field result)))
    (is (= "\"jointable2\".\"field\"" (:path result)))
    (is (= "\"jointable2\".\"field\"::text" (:lhs result)))
    (is (= "LIKE" (get-in result [:appended :operator])))))

(deftest with-appendix-should-cast-mixed-json-paths-with-a-schema-and-table
  (let [result (p/with-appendix db join-source "myschema.jointable2.json.array[1].field.array[2]::boolean LIKE" o/operations)]
    (is (= "myschema" (:schema result)))
    (is (= "jointable2" (:relation result)))
    (is (= "json" (:field result)))
    (is (= "\"jointable2\".\"json\"" (:path result)))
    (is (= "(\"jointable2\".\"json\"#>>'{array,1,field,array,2}')::boolean" (:lhs result)))
    (is (= "LIKE" (get-in result [:appended :operator])))))

