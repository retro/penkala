(ns com.verybigthings.penkala.relation.cte-test
  (:require [clojure.test :refer [use-fixtures deftest is]]
            [com.verybigthings.penkala.next-jdbc :refer [select! select-one!]]
            [com.verybigthings.penkala.relation :as r]
            [com.verybigthings.penkala.helpers :as h]
            [com.verybigthings.penkala.test-helpers :as th :refer [*env*]]))

(use-fixtures :once (partial th/reset-db-fixture "data-all"))

(deftest it-returns-products-in-1-and-2
  (let [products-cte (r/as-cte
                      (-> *env* :products
                          (r/select [:id])
                          (r/where [:or [:= :id 1] [:= :id 2]])))
        res (select! *env* (-> *env*
                               :products
                               (r/select [:id])
                               (r/where [:in :id products-cte])))]
    (is (= 2 (count res)))
    (is (= #{1 2} (->> res (map :products/id) set)))))

(deftest cte-ordering
  (let [products-1-cte (r/as-cte
                        (-> (:products *env*)
                            (r/select [:id])
                            (r/where [:or [:= :id 1] [:= :id 2]])))
        products-2-cte (r/as-cte
                        (-> products-1-cte
                            (r/select [:id])
                            (r/where [:= :id 2])))
        products-1-2-cte (r/as-cte
                          (-> (:products *env*)
                              (r/select [:id])
                              (r/where [:or [:in :id products-1-cte] [:in :id products-2-cte]])))
        products-3-cte (r/as-cte
                        (-> (:products *env*)
                            (r/select [:id])
                            (r/where [:= :id 3])))
        products (-> (:products *env*)
                     (r/select [:id])
                     (r/where [:or [:in :id products-3-cte] [:in :id products-1-2-cte]]))
        res (select! *env* products)]
    (is (= 3 (count res)))
    (is (= #{1 2 3} (->> res (map :products/id) set)))))