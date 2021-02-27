(ns com.verybigthings.penkala.relation.insert-test
  (:require [clojure.test :refer :all]
            [com.verybigthings.penkala.next-jdbc :refer [insert! select!]]
            [com.verybigthings.penkala.relation :as r]
            [com.verybigthings.penkala.test-helpers :as th :refer [*env*]]
            [com.verybigthings.penkala.decomposition :refer [map->DecompositionSchema]]
            [jsonista.core :as j])
  (:import [org.postgresql.util PSQLException]))

(use-fixtures :each (partial th/reset-db-fixture "updatables"))

(deftest it-inserts-a-record-and-returns-a-map
  (let [normal-pk (:normal-pk *env*)
        ins-normal-pk (r/->insertable normal-pk)
        res (insert! *env* ins-normal-pk {:field-1 "epsilon"})]
    (is (= #:normal-pk{:field-1 "epsilon"
                       :json-field nil
                       :field-2 nil
                       :array-of-json nil
                       :array-field nil}
          (dissoc res :normal-pk/id)))))

(deftest it-inserts-a-record-and-returns-map-with-custom-projection
  (let [normal-pk (:normal-pk *env*)
        ins-normal-pk (-> (r/->insertable normal-pk)
                        (r/returning [:field-1]))
        res (insert! *env* ins-normal-pk {:field-1 "epsilon"})]
    (is (= #:normal-pk{:field-1 "epsilon"}
          (dissoc res :normal-pk/id)))))

(deftest it-inserts-a-record-and-returns-a-result-without-projection
  (let [normal-pk (:normal-pk *env*)
        ins-normal-pk (-> (r/->insertable normal-pk)
                        (r/returning nil))
        res (insert! *env* ins-normal-pk {:field-1 "epsilon111"})]
    (is (= #:next.jdbc{:update-count 1} res))))

(deftest it-inserts-multiple-normal-ok-records-and-returns-a-vector-of-results
  (let [normal-pk (:normal-pk *env*)
        ins-normal-pk (r/->insertable normal-pk)
        res (insert! *env* ins-normal-pk [{:field-1 "zeta"} {:field-1 "eta"}])]
    (is (= [#:normal-pk{:field-1 "zeta"
                        :json-field nil
                        :field-2 nil
                        :array-of-json nil
                        :array-field nil}
            #:normal-pk{:field-1 "eta"
                        :json-field nil
                        :field-2 nil
                        :array-of-json nil
                        :array-field nil}]
          (mapv #(dissoc % :normal-pk/id) res)))))

(deftest it-combines-keys-of-partial-maps-on-insert
  (let [normal-pk (:normal-pk *env*)
        ins-normal-pk (r/->insertable normal-pk)
        res (insert! *env* ins-normal-pk [{:field-1 "theta" :field-2 "ateht"}
                                          {:field-1 "iota" :array-field ["one" "two"]}])]
    (is (= [#:normal-pk{:field-1 "theta"
                        :json-field nil
                        :field-2 "ateht"
                        :array-of-json nil
                        :array-field nil}
            #:normal-pk{:field-1 "iota"
                        :json-field nil
                        :field-2 nil
                        :array-of-json nil
                        :array-field ["one" "two"]}]
          (mapv #(dissoc % :normal-pk/id) res)))))

(deftest it-throws-when-a-partial-record-excludes-a-constrained-field
  (let [normal-pk (:normal-pk *env*)
        ins-normal-pk (r/->insertable normal-pk)]
    (is (thrown? org.postgresql.util.PSQLException
          (insert! *env* ins-normal-pk [{:field-1 "ephemeral"}
                                        {:field-2 "insufficient"}])))))

(deftest it-inserts-array-fields
  (let [normal-pk (:normal-pk *env*)
        ins-normal-pk (r/->insertable normal-pk)
        res (insert! *env* ins-normal-pk {:field-1 "kappa" :array-field ["one" "two"]})]
    (is (= #:normal-pk{:field-1 "kappa"
                       :json-field nil
                       :field-2 nil
                       :array-of-json nil
                       :array-field ["one" "two"]}
          (dissoc res :normal-pk/id)))))

(deftest it-inserts-empty-array-fields
  (let [normal-pk (:normal-pk *env*)
        ins-normal-pk (r/->insertable normal-pk)
        res (insert! *env* ins-normal-pk {:field-1 "mu" :array-field []})]
    (is (= #:normal-pk{:field-1 "mu"
                       :json-field nil
                       :field-2 nil
                       :array-of-json nil
                       :array-field []}
          (dissoc res :normal-pk/id)))))

(deftest it-inserts-json-arrays
  (let [normal-pk (:normal-pk *env*)
        ins-normal-pk (r/->insertable normal-pk)
        res (insert! *env* ins-normal-pk {:field-1 "nu" :json-field ["one" "two" "three"]})]
    (is (= #:normal-pk{:field-1 "nu"
                       :json-field ["one" "two" "three"]
                       :field-2 nil
                       :array-of-json nil
                       :array-field nil}
          (dissoc res :normal-pk/id)))))

(deftest it-inserts-a-record-with-uuid-pk
  (let [uuid-pk (:uuid-pk *env*)
        ins-uuid-pk (r/->insertable uuid-pk)
        res (insert! *env* ins-uuid-pk {:field-1 "a"})]
    (is (= #:uuid-pk{:field-1 "a"}
          (dissoc res :uuid-pk/id)))))

(deftest it-inserts-a-record-into-a-table-with-a-cased-name
  (let [cased-name (:cased-name *env*)
        ins-cased-name (r/->insertable cased-name)
        res (insert! *env* ins-cased-name {:field-1 "b"})]
    (is (= #:cased-name{:field-1 "b"}
          (dissoc res :cased-name/id)))))

(deftest it-inserts-into-a-qualifying-view
  (let [normal-as (:normal-as *env*)
        ins-normal-as (r/->insertable normal-as)
        res (insert! *env* ins-normal-as {:field-1 "aardvark"})]
    (is (= #:normal-as{:field-1 "aardvark"
                       :array-field nil
                       :array-of-json nil
                       :field-2 nil
                       :json-field nil}
          (dissoc res :normal-as/id)))))

(deftest it-inserts-into-a-view-and-returns-a-result-outside-the-scope
  (let [normal-as (:normal-as *env*)
        ins-normal-as (r/->insertable normal-as)
        res (insert! *env* ins-normal-as {:field-1 "pangolin"})]
    (is (= #:normal-as{:field-1 "pangolin"
                       :array-field nil
                       :array-of-json nil
                       :field-2 nil
                       :json-field nil}
          (dissoc res :normal-as/id)))
    (is (= nil (select! *env* (-> normal-as
                                (r/where [:= :field-1 "pangolin"])))))))

(deftest it-throws-if-not-insertable
  (let [normal-as (-> (:normal-as *env*)
                    (update-in [:spec :is-insertable-into] not))]
    (is (thrown? clojure.lang.ExceptionInfo
          (r/->insertable normal-as)))))