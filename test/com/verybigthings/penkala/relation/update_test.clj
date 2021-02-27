(ns com.verybigthings.penkala.relation.update-test
  (:require [clojure.test :refer :all]
            [com.verybigthings.penkala.next-jdbc :refer [update! select-one!]]
            [com.verybigthings.penkala.relation :as r]
            [com.verybigthings.penkala.test-helpers :as th :refer [*env*]]
            [com.verybigthings.penkala.decomposition :refer [map->DecompositionSchema]]))

(use-fixtures :each (partial th/reset-db-fixture "updatables"))

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