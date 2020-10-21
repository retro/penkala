(ns com.verybigthings.penkala.util.parse-key-test
  (:require [clojure.test :refer :all]
            [com.verybigthings.penkala.util.parse-key :as p]))

(def source
  {:path "mytable"
   :schema "public"
   :name "mytable"
   :delimited-name "\"mytable\""
   :delimited-schema "\"public\""
   :delimited-full-name "\"mytable\""
   :column-names [:id :field :col1 :col2 :body]
   :is-mat-view false
   :pk [:id]
   :insertable true
   })

(def db
  {:schema "public"})

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