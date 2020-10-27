(ns com.verybigthings.penkala.relation.find-test
  (:require [clojure.test :refer :all]
            [com.verybigthings.penkala.db :refer [query query-one]]
            [com.verybigthings.penkala.relation :as r]
            [com.verybigthings.penkala.test-helpers :as th :refer [db-uri *db*]]))

(use-fixtures :once (partial th/reset-db-fixture "data-all"))

(deftest it-returns-all-records-by-default
  (let [res (query db-uri *db* :relation/products)]
    (is (= 4 (count res)))))

(deftest it-finds-by-numeric-id
  (let [products (:relation/products *db*)
        res (query-one db-uri *db* (-> products (r/where {:id 1})))]
    (is (map? res))
    (is (= 1 (:id res)))))

(deftest it-finds-by-uuid-id
  (let [orders (:relation/orders *db*)
        order-1 (query-one db-uri *db* orders)
        order-id (:id order-1)
        order-2 (query-one db-uri *db* (-> orders (r/where {:id order-id})))]
    (is (map? order-1))
    (is (map? order-2))
    (is (= order-1 order-2))))

(deftest it-finds-by-multiple-conditions-1
  (let [products (:relation/products *db*)
        res-1 (query-one db-uri *db* (-> products (r/where [:and {:id 2 :name "Product 2"}
                                                            [:and [:or [:and {}]]]
                                                            [:or {:in_stock false} [:and {"price >" 12}]]])))
        res-2 (query-one db-uri *db* (-> products (r/where [:and {:id 2 :name "Product 2"}
                                                            [:or {:in_stock false} {"price >" 12}]])))]
    (is (= 2 (:id res-1) (:id res-2)))
    (is (= res-1 res-2))))