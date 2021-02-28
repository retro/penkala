(ns com.verybigthings.penkala.relation.delete-test
  (:require [clojure.test :refer :all]
            [com.verybigthings.penkala.next-jdbc :refer [delete! select-one! insert!]]
            [com.verybigthings.penkala.relation :as r]
            [com.verybigthings.penkala.test-helpers :as th :refer [*env*]]
            [com.verybigthings.penkala.decomposition :refer [map->DecompositionSchema]]))

(use-fixtures :each (partial th/reset-db-fixture "singletable"))

(deftest it-deletes
  (let [products (:products *env*)
        del-products (-> (r/->deletable products)
                        (r/where [:= :id 3]))
        res (delete! *env* del-products)]
    (is (= [#:products{:description nil,
                       :tags nil,
                       :string "three",
                       :id 3,
                       :specs nil,
                       :case-name nil,
                       :price 0.00M}]
          (mapv #(dissoc % :products/uuid) res)))))

(deftest it-deletes-with-using
  (let [products (:products *env*)
        del-products (-> (r/->deletable products)
                       (r/using (r/where products [:= :id 3]) :other-products)
                       (r/where [:= :id :other-products/id]))
        res (delete! *env* del-products)]
    (is (= [#:products{:description nil,
                       :tags nil,
                       :string "three",
                       :id 3,
                       :specs nil,
                       :case-name nil,
                       :price 0.00M}]
          (mapv #(dissoc % :products/uuid) res)))))