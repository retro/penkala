(ns com.verybigthings.penkala.relation.update-test
  (:require [clojure.test :refer [use-fixtures deftest is]]
            [com.verybigthings.penkala.next-jdbc :refer [update! insert! select-one!]]
            [com.verybigthings.penkala.relation :as r]
            [com.verybigthings.penkala.test-helpers :as th :refer [*env*]]))

(use-fixtures :each (partial th/reset-db-fixture "updatables"))

(deftest it-throws-for-unexisting-relation
  (is (thrown? clojure.lang.ExceptionInfo
               (update! *env* :unexisting-relation [{:field-1 "zeta"}]))))

(deftest it-updates
  (let [normal-pk (:normal-pk *env*)
        upd-normal-pk (-> (r/->updatable normal-pk)
                          (r/where [:= :id 1]))
        res (update! *env* upd-normal-pk {:field-1 "zeta"})]
    (is (= [#:normal-pk{:field-1 "zeta"
                        :json-field nil
                        :field-2 nil
                        :array-of-json nil
                        :array-field nil
                        :id 1}]
           res))))

(deftest it-updates-and-can-use-returning-all-but
  (let [normal-pk (:normal-pk *env*)
        upd-normal-pk (-> (r/->updatable normal-pk)
                          (r/returning-all-but [:json-field :field-2])
                          (r/where [:= :id 1]))
        res (update! *env* upd-normal-pk {:field-1 "zeta"})]
    (is (= [#:normal-pk{:field-1 "zeta"
                        :array-of-json nil
                        :array-field nil
                        :id 1}]
           res))))

(deftest it-updates-field-to-a-null-value
  (let [normal-pk (:normal-pk *env*)
        upd-normal-pk (-> (r/->updatable normal-pk)
                          (r/where [:= :id 1]))
        res (update! *env* upd-normal-pk {:field-1 "zeta" :field-2 "field-2"})]
    (is (= [#:normal-pk{:field-1 "zeta"
                        :json-field nil
                        :field-2 "field-2"
                        :array-of-json nil
                        :array-field nil
                        :id 1}]
           res))
    (let [res-2 (update! *env* upd-normal-pk {:field-2 nil})]
      (is (= [#:normal-pk{:field-1 "zeta"
                          :json-field nil
                          :field-2 nil
                          :array-of-json nil
                          :array-field nil
                          :id 1}]
             res-2)))))

(deftest it-updates-multiple-columns
  (let [normal-pk (:normal-pk *env*)
        upd-normal-pk (-> (r/->updatable normal-pk)
                          (r/where [:= :id 1]))
        res (update! *env* upd-normal-pk {:field-1 "zeta" :field-2 "beta"})]
    (is (= [#:normal-pk{:field-1 "zeta"
                        :json-field nil
                        :field-2 "beta"
                        :array-of-json nil
                        :array-field nil
                        :id 1}]
           res))))

(deftest it-updates-with-from-table
  (let [normal-pk (:normal-pk *env*)
        normal-pk-id-1 (select-one! *env* (-> normal-pk
                                              (r/where [:= :id 1])))
        upd-normal-pk (-> (r/->updatable normal-pk)
                          (r/from normal-pk :normal-pk-2)
                          (r/where [:and [:= :id 1]
                                    [:= :id :normal-pk-2/id]]))
        res (update! *env* upd-normal-pk {:field-1 [:concat "from-outside" "<->" :normal-pk-2/field-1]})]
    (is (= [#:normal-pk{:field-1 (str "from-outside<->" (:normal-pk/field-1 normal-pk-id-1))
                        :json-field nil
                        :field-2 nil
                        :array-of-json nil
                        :array-field nil
                        :id 1}]
           res))))

(deftest it-updates-with-from-tables
  (let [normal-pk (:normal-pk *env*)
        normal-pk-id-1 (select-one! *env* (-> normal-pk
                                              (r/where [:= :id 1])))
        upd-normal-pk (-> (r/->updatable normal-pk)
                          (r/from normal-pk :normal-pk-2)
                          (r/from normal-pk :normal-pk-3)
                          (r/where [:and [:= :id 1]
                                    [:= :id :normal-pk-2/id]
                                    [:= :id :normal-pk-3/id]]))
        res (update! *env* upd-normal-pk {:field-1 [:concat "from-outside" "<->" :normal-pk-2/field-1 "<->" :normal-pk-3/field-1]})]
    (is (= [#:normal-pk{:field-1 (str "from-outside<->" (:normal-pk/field-1 normal-pk-id-1) "<->" (:normal-pk/field-1 normal-pk-id-1))
                        :json-field nil
                        :field-2 nil
                        :array-of-json nil
                        :array-field nil
                        :id 1}]
           res))))

(deftest it-updates-without-returning-projection
  (let [normal-pk (:normal-pk *env*)
        upd-normal-pk (-> (r/->updatable normal-pk)
                          (r/returning nil)
                          (r/where [:= :id 1]))
        res (update! *env* upd-normal-pk {:field-1 "zeta"})]
    (is (= {:next.jdbc/update-count 1} res))))

(deftest it-updates-multiple-records
  (let [normal-pk (:normal-pk *env*)
        upd-normal-pk (-> (r/->updatable normal-pk)
                          (r/where [:> :id 1]))
        res (update! *env* upd-normal-pk {:field-1 "eta"})]
    (is (= [#:normal-pk{:field-1 "eta"
                        :json-field nil
                        :field-2 nil
                        :array-of-json nil
                        :array-field nil
                        :id 2}
            #:normal-pk{:field-1 "eta"
                        :json-field nil
                        :field-2 nil
                        :array-of-json nil
                        :array-field nil
                        :id 3}]
           res))))

(deftest it-updates-correctly-to-false-value
  (let [booleans (:booleans *env*)
        ins-booleans (r/->insertable booleans)
        res (insert! *env* ins-booleans {:value true})
        upd-booleans (-> booleans
                         r/->updatable
                         (r/where [:= :id (:booleans/id res)]))
        res-2 (update! *env* upd-booleans {:value false})]
    (is (= #:booleans{:id 1 :value true} res))
    (is (= [#:booleans{:id 1 :value false}] res-2))))

(deftest it-can-use-nil-values-in-value-expressions
  (let [booleans (:booleans *env*)
        ins-booleans (r/->insertable booleans)
        res (insert! *env* ins-booleans {:value true})
        upd-booleans (-> booleans
                         r/->updatable
                         (r/where [:= :id nil]))
        res-2 (update! *env* upd-booleans {:value false})]
    (is (= #:booleans{:id 1 :value true} res))
    (is (= nil res-2))))