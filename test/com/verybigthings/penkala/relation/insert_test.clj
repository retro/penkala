(ns com.verybigthings.penkala.relation.insert-test
  (:require [clojure.test :refer [deftest is use-fixtures]]
            [com.verybigthings.penkala.next-jdbc :refer [insert! select!]]
            [com.verybigthings.penkala.relation :as r]
            [com.verybigthings.penkala.test-helpers :as th :refer [*env*]]))

(use-fixtures :each (partial th/reset-db-fixture "updatables"))

(deftest it-inserts-a-record-and-returns-a-map
  (let [normal-pk     (:normal-pk *env*)
        ins-normal-pk (r/->insertable normal-pk)
        res           (insert! *env* ins-normal-pk {:field-1 "epsilon"})]
    (is (= #:normal-pk{:field-1 "epsilon"
                       :json-field nil
                       :field-2 nil
                       :array-of-json nil
                       :array-field nil}
           (dissoc res :normal-pk/id)))))

(deftest it-inserts-a-record-and-returns-map-with-custom-projection
  (let [normal-pk     (:normal-pk *env*)
        ins-normal-pk (-> (r/->insertable normal-pk)
                          (r/returning [:field-1]))
        res           (insert! *env* ins-normal-pk {:field-1 "epsilon"})]
    (is (= #:normal-pk{:field-1 "epsilon"}
           (dissoc res :normal-pk/id)))))

(deftest it-inserts-a-record-and-returns-a-result-without-projection
  (let [normal-pk     (:normal-pk *env*)
        ins-normal-pk (-> (r/->insertable normal-pk)
                          (r/returning nil))
        res           (insert! *env* ins-normal-pk {:field-1 "epsilon111"})]
    (is (= #:next.jdbc{:update-count 1} res))))

(deftest it-inserts-multiple-normal-ok-records-and-returns-a-vector-of-results
  (let [normal-pk     (:normal-pk *env*)
        ins-normal-pk (r/->insertable normal-pk)
        res           (insert! *env* ins-normal-pk [{:field-1 "zeta"} {:field-1 "eta"}])]
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
  (let [normal-pk     (:normal-pk *env*)
        ins-normal-pk (r/->insertable normal-pk)
        res           (insert! *env* ins-normal-pk [{:field-1 "theta" :field-2 "ateht"}
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
  (let [normal-pk     (:normal-pk *env*)
        ins-normal-pk (r/->insertable normal-pk)]
    (is (thrown? org.postgresql.util.PSQLException
                 (insert! *env* ins-normal-pk [{:field-1 "ephemeral"}
                                               {:field-2 "insufficient"}])))))

(deftest it-throws-for-unexisting-relation
  (is (thrown? clojure.lang.ExceptionInfo
               (insert! *env* :unexisting-relation [{:field-1 "ephemeral"}
                                                    {:field-2 "insufficient"}]))))

(deftest it-inserts-array-fields
  (let [normal-pk     (:normal-pk *env*)
        ins-normal-pk (r/->insertable normal-pk)
        res           (insert! *env* ins-normal-pk {:field-1 "kappa" :array-field ["one" "two"]})]
    (is (= #:normal-pk{:field-1 "kappa"
                       :json-field nil
                       :field-2 nil
                       :array-of-json nil
                       :array-field ["one" "two"]}
           (dissoc res :normal-pk/id)))))

(deftest it-inserts-empty-array-fields
  (let [normal-pk     (:normal-pk *env*)
        ins-normal-pk (r/->insertable normal-pk)
        res           (insert! *env* ins-normal-pk {:field-1 "mu" :array-field []})]
    (is (= #:normal-pk{:field-1 "mu"
                       :json-field nil
                       :field-2 nil
                       :array-of-json nil
                       :array-field []}
           (dissoc res :normal-pk/id)))))

(deftest it-inserts-json-arrays
  (let [normal-pk     (:normal-pk *env*)
        ins-normal-pk (r/->insertable normal-pk)
        res           (insert! *env* ins-normal-pk {:field-1 "nu" :json-field ["one" "two" "three"]})]
    (is (= #:normal-pk{:field-1 "nu"
                       :json-field ["one" "two" "three"]
                       :field-2 nil
                       :array-of-json nil
                       :array-field nil}
           (dissoc res :normal-pk/id)))))

(deftest it-inserts-a-record-with-uuid-pk
  (let [uuid-pk     (:uuid-pk *env*)
        ins-uuid-pk (r/->insertable uuid-pk)
        res         (insert! *env* ins-uuid-pk {:field-1 "a"})]
    (is (= #:uuid-pk{:field-1 "a"}
           (dissoc res :uuid-pk/id)))))

(deftest it-inserts-a-record-into-a-table-with-a-cased-name
  (let [cased-name     (:cased-name *env*)
        ins-cased-name (r/->insertable cased-name)
        res            (insert! *env* ins-cased-name {:field-1 "b"})]
    (is (= #:cased-name{:field-1 "b"}
           (dissoc res :cased-name/id)))))

(deftest it-inserts-into-a-qualifying-view
  (let [normal-as     (:normal-as *env*)
        ins-normal-as (r/->insertable normal-as)
        res           (insert! *env* ins-normal-as {:field-1 "aardvark"})]
    (is (= #:normal-as{:field-1 "aardvark"
                       :array-field nil
                       :array-of-json nil
                       :field-2 nil
                       :json-field nil}
           (dissoc res :normal-as/id)))))

(deftest it-inserts-into-a-view-and-returns-a-result-outside-the-scope
  (let [normal-as     (:normal-as *env*)
        ins-normal-as (r/->insertable normal-as)
        res           (insert! *env* ins-normal-as {:field-1 "pangolin"})]
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

(deftest it-throws-if-invalid-insertable
  (is (thrown? clojure.lang.ExceptionInfo
               (r/->insertable :invalid-relation))))

(deftest it-can-handle-conflicts-with-nothing
  (let [things     (:things *env*)
        ins-things (r/->insertable things)]
    (insert! *env* ins-things {:stuff "stuff" :name "NAME"})
    (let [res (insert! *env*
                       (-> ins-things
                           r/on-conflict-do-nothing)
                       {:stuff "stuff" :name "NAME"})]
      (is (nil? res)))
    (let [res (insert! *env*
                       (-> ins-things
                           (r/on-conflict-do-nothing
                            [:stuff [:lower :name]]))
                       {:stuff "stuff" :name "NAME"})]
      (is (nil? res)))
    (let [res (insert! *env*
                       (-> ins-things
                           (r/on-conflict-do-nothing
                            [:stuff [:lower :name]]
                            [:= :name "NAME"]))
                       {:stuff "stuff" :name "NAME"})]
      (is (nil? res)))))

(deftest it-can-handle-conflicts-with-nothing-with-explicit-on-constraint
  (let [things     (:things *env*)
        ins-things (r/->insertable things)]
    (insert! *env* ins-things {:stuff "stuff" :name "NAME"})
    (let [res (insert! *env*
                       (-> ins-things
                           r/on-conflict-do-nothing)
                       {:stuff "stuff" :name "NAME"})]
      (is (nil? res)))
    (let [res (insert! *env*
                       (-> ins-things
                           (r/on-conflict-do-nothing
                            [:on-constraint "things_pkey"]))
                       {:stuff "stuff" :name "NAME" :id 1})]
      (is (nil? res)))))


(deftest it-can-handle-conflicts-with-update
  (let [things     (:things *env*)
        ins-things (r/->insertable things)]
    (insert! *env* ins-things {:stuff "stuff" :name "NAME"})
    (let [res (insert! *env*
                       (-> ins-things
                           (r/on-conflict-do-update
                            [:stuff [:lower :name]]
                            {:stuff [:concat :excluded/stuff "1"]
                             :name [:concat :excluded/name "1"]}))
                       {:stuff "stuff"
                        :name "NAME"})]
      (is (= #:things{:stuff "stuff1"
                      :name "NAME1"
                      :id 1}
             res)))
    (let [res (insert! *env*
                       (-> ins-things
                           (r/on-conflict-do-update
                            [:stuff [:lower :name]]
                            {:stuff [:concat :excluded/stuff "2"]
                             :name [:concat :excluded/name "2"]}
                            [:= :name "NAME1"]))
                       {:stuff "stuff1"
                        :name "NAME1"})]
      (is (= #:things{:stuff "stuff12"
                      :name "NAME12"
                      :id 1}
             res)))))

(deftest it-can-handle-conflicts-with-update-with-explicit-on-constraint
  (let [things     (:things *env*)
        ins-things (r/->insertable things)]
    (insert! *env* ins-things {:stuff "stuff" :name "NAME"})
    (let [res (insert! *env*
                       (-> ins-things
                           (r/on-conflict-do-update
                            [:on-constraint "things_pkey"]
                            {:stuff [:concat :excluded/stuff "1"]
                             :name [:concat :excluded/name "1"]
                             :id 1}))
                       {:stuff "stuff"
                        :name "NAME"
                        :id 1})]
      (is (= #:things{:stuff "stuff1"
                      :name "NAME1"
                      :id 1}
             res)))))

(deftest it-can-use-value-expression-for-inserts
  (let [things     (:things *env*)
        ins-things (r/->insertable things)
        res        (insert! *env* ins-things {:stuff [:lower "STUFF"] :name [:upper "name"]})]
    (is (= #:things{:stuff "stuff" :name "NAME" :id 1}
           res))))