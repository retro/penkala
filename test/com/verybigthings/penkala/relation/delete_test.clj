(ns com.verybigthings.penkala.relation.delete-test
  (:require [clojure.test :refer [use-fixtures deftest is]]
            [com.verybigthings.penkala.next-jdbc :refer [delete!]]
            [com.verybigthings.penkala.relation :as r]
            [com.verybigthings.penkala.test-helpers :as th :refer [*env*]]))

(use-fixtures :each (partial th/reset-db-fixture "singletable"))

(deftest it-throws-for-unexisting-relation
  (is (thrown? clojure.lang.ExceptionInfo
               (delete! *env* :unexisting-relation))))

(deftest it-deletes
  (let [products (:products *env*)
        del-products (-> (r/->deletable products)
                         (r/where [:= :id 3]))
        res (delete! *env* del-products)]
    (is (= [#:products{:description nil
                       :tags nil
                       :string "three"
                       :id 3
                       :specs nil
                       :case-name nil
                       :price 0.00M}]
           (mapv #(dissoc % :products/uuid) res)))))

(deftest it-deletes-and-can-use-returning-all-but
  (let [products (:products *env*)
        del-products (-> (r/->deletable products)
                         (r/returning-all-but [:string :tags])
                         (r/where [:= :id 3]))
        res (delete! *env* del-products)]
    (is (= [#:products{:description nil
                       :id 3
                       :specs nil
                       :case-name nil
                       :price 0.00M}]
           (mapv #(dissoc % :products/uuid) res)))))

(deftest it-deletes-with-using
  (let [products (:products *env*)
        del-products (-> (r/->deletable products)
                         (r/using (r/where products [:= :id 3]) :other-products)
                         (r/where [:= :id :other-products/id]))
        res (delete! *env* del-products)]
    (is (= [#:products{:description nil
                       :tags nil
                       :string "three"
                       :id 3
                       :specs nil
                       :case-name nil
                       :price 0.00M}]
           (mapv #(dissoc % :products/uuid) res)))))

(deftest it-deletes-with-using-multiple
  (let [products (:products *env*)
        del-products (-> (r/->deletable products)
                         (r/using (r/where products [:= :id 3]) :other-products-1)
                         (r/using (r/where products [:= :id 3]) :other-products-2)
                         (r/where [:and
                                   [:= :id :other-products-1/id]
                                   [:= :id :other-products-2/id]]))
        res (delete! *env* del-products)]
    (is (= [#:products{:description nil
                       :tags nil
                       :string "three"
                       :id 3
                       :specs nil
                       :case-name nil
                       :price 0.00M}]
           (mapv #(dissoc % :products/uuid) res)))))