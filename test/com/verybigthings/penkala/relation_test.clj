(ns com.verybigthings.penkala.relation-test
  (:require [clojure.test :refer :all]
            [com.verybigthings.penkala.relation :as r]))

(deftest it-should-use-spec-path-if-there-is-one
  (let [entity (r/make-relation {}
                 {:loader :tables
                  :path "one.test_entity"
                  :name "test_entity"
                  :schema "one"})]
    (is (= {:relation/path "one.test_entity"
            :relation/name "test_entity"
            :db/schema "one"
            :db.schema/current nil
            :relation/loader :tables
            :relation/delimited-name "\"test_entity\""
            :relation/delimited-schema "\"one\""
            :relation/delimited-full-name "\"one\".\"test_entity\""
            :relation/column-names #{}
            :relation/columns []
            :relation/fks #{}
            :relation/is-mat-view false
            :relation/pk nil}
          entity))))

(deftest it-should-create-a-base-entity-from-a-spec
  (let [entity (r/make-relation {}
                 {:loader :tables
                  :name "test_entity"
                  :schema "one"})]
    (is (= {:relation/path "one.test_entity"
            :relation/name "test_entity"
            :db/schema "one"
            :db.schema/current nil
            :relation/loader :tables
            :relation/delimited-name "\"test_entity\""
            :relation/delimited-schema "\"one\""
            :relation/delimited-full-name "\"one\".\"test_entity\""
            :relation/column-names #{}
            :relation/columns []
            :relation/fks #{}
            :relation/is-mat-view false
            :relation/pk nil}
          entity))))

(deftest it-should-default-to-the-current-schema
  (let [entity (r/make-relation {:db/schema "other"}
                 {:loader :tables
                  :name "test_entity"
                  :schema "other"})]
    (is (= {:relation/path "test_entity"
            :relation/name "test_entity"
            :db/schema "other"
            :db.schema/current "other"
            :relation/loader :tables
            :relation/delimited-name "\"test_entity\""
            :relation/delimited-schema "\"other\""
            :relation/delimited-full-name "\"test_entity\""
            :relation/column-names #{}
            :relation/columns []
            :relation/fks #{}
            :relation/is-mat-view false
            :relation/pk nil}
          entity))))