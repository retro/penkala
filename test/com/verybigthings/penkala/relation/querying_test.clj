(ns com.verybigthings.penkala.relation.querying-test
  (:require [clojure.test :refer [use-fixtures deftest is]]
            [com.verybigthings.penkala.next-jdbc :refer [select! select-one!]]
            [com.verybigthings.penkala.relation :as r]
            [com.verybigthings.penkala.helpers :as h]
            [com.verybigthings.penkala.test-helpers :as th :refer [*env*]]))

(use-fixtures :once (partial th/reset-db-fixture "data-all"))

(deftest if-throws-for-unexisting-relation
  (is (thrown? clojure.lang.ExceptionInfo (select! *env* :unexisting-relation))))

(deftest it-returns-all-records-by-default
  (let [res (select! *env* :products)]
    (is (= 4 (count res)))))

(deftest it-finds-by-numeric-id
  (let [products (:products *env*)
        res      (select-one! *env* (-> products (r/where [:= 1 :id])))]
    (is (map? res))
    (is (= 1 (:products/id res)))))

(deftest it-finds-by-uuid-id
  (let [orders   (:orders *env*)
        order-1  (select-one! *env* orders)
        order-id (:orders/id order-1)
        order-2  (select-one! *env* (-> orders (r/where [:= :id order-id])))]
    (is (map? order-1))
    (is (map? order-2))
    (is (= order-1 order-2))))

(deftest it-finds-by-multiple-conditions-1
  (let [products (:products *env*)
        res      (select-one! *env* (-> products (r/where [:and [:= :id 2] [:= :name "Product 2"]])))]
    (is (= 2 (:products/id res)))))

(deftest it-returns-products-with-id-greater-than-2
  (let [res (select! *env* (-> *env* :products (r/where [:> :id 2])))]
    (is (= 2 (count res)))
    (is (= #{3 4} (->> res (map :products/id) set)))))

(deftest it-returns-products-with-id-less-than-2
  (let [res (select! *env* (-> *env* :products (r/where [:< :id 2])))]
    (is (= 1 (count res)))
    (is (= #{1} (->> res (map :products/id) set)))))

(deftest it-returns-products-in-1-and-2
  (let [res (select! *env* (-> *env* :products (r/where [:in :id [1 2]])))
        res2 (select! *env* (-> *env* :products (r/where [:in :id (-> *env* :products
                                                                      (r/select [:id])
                                                                      (r/where [:or [:= :id 1] [:= :id 2]]))])))]
    (is (= 2 (count res) (count res2)))
    (is (= #{1 2} (->> res (map :products/id) set) (->> res2 (map :products/id) set)))))

(deftest it-returns-products-not-in-1-and-2
  (let [res (select! *env* (-> *env* :products (r/where [:not-in :id [1 2]])))
        res2 (select! *env* (-> *env* :products (r/where [:not-in :id (-> *env* :products
                                                                          (r/select [:id])
                                                                          (r/where [:or [:= :id 1] [:= :id 2]]))])))]
    (is (= 2 (count res) (count res2)))
    (is (= #{3 4} (->> res (map :products/id) set) (->> res2 (map :products/id) set)))))

(deftest it-returns-products-with-is-null
  (let [res (select! *env* (-> *env* :products (r/where [:is-null :tags])))]
    (is (= 1 (count res)))
    (is (= #{1} (->> res (map :products/id) set)))))

(deftest it-returns-products-with-is-not-null
  (let [res (select! *env* (-> *env* :products (r/where [:is-not-null :tags])))]
    (is (= 3 (count res)))
    (is (= #{2 3 4} (->> res (map :products/id) set)))))

(deftest it-returns-products-using-distinct-from
  (let [res (select! *env* (-> *env* :products (r/where [:is-distinct-from :tags (h/ql "{tag1,tag2}")])))
        res2 (select! *env* (-> *env* :products (r/where [:is-distinct-from :tags (h/array-literal ["tag1" "tag2"])])))
        res3 (select! *env* (-> *env* :products (r/where [:is-distinct-from :tags ["tag1" "tag2"]])))]
    (is (= 3 (count res) (count res2) (count res3)))
    (is (= #{1 3 4}
           (->> res (map :products/id) set)
           (->> res2 (map :products/id) set)
           (->> res3 (map :products/id) set)))))

(deftest it-returns-products-using-not-distinct-from
  (let [res (select! *env* (-> *env* :products (r/where [:is-not-distinct-from :tags (h/ql "{tag1,tag2}")])))
        res2 (select! *env* (-> *env* :products (r/where [:is-not-distinct-from :tags (h/array-literal ["tag1" "tag2"])])))
        res3 (select! *env* (-> *env* :products (r/where [:is-not-distinct-from :tags ["tag1" "tag2"]])))]
    (is (= 1 (count res) (count res2) (count res3)))
    (is (= #{2}
           (->> res (map :products/id) set)
           (->> res2 (map :products/id) set)
           (->> res3 (map :products/id) set)))))

(deftest it-returns-products-with-a-compound-where
  (let [res (select! *env* (-> *env* :products (r/where [:and [:= :id 1] [:= :price 12.0] [:is-null :tags]])))]
    (is (= 1 (count res)))
    (is (= #{1} (->> res (map :products/id) set)))))

(deftest it-finds-a-product-by-a-search-string-with-like
  (let [res (select-one! *env* (-> *env* :products (r/where [:like :name "%odu_t 2"])))]
    (is (= 2 (:products/id res)))
    (is (= "Product 2" (:products/name res)))))

(deftest it-finds-a-product-by-a-search-string-with-not-like
  (let [res (select-one! *env* (-> *env* :products (r/where [:not-like :name "%odu_t 2"])))]
    (is (:products/id res))
    (is (not= 2 (:products/id res)))
    (is (not= "Product 2" (:products/name res)))))

(deftest it-finds-a-product-by-a-search-string-with-ilike
  (let [res (select-one! *env* (-> *env* :products (r/where [:ilike :name "%OdU_t 2"])))]
    (is (= 2 (:products/id res)))
    (is (= "Product 2" (:products/name res)))))

(deftest it-finds-a-product-by-a-search-string-with-not-ilike
  (let [res (select-one! *env* (-> *env* :products (r/where [:not-ilike :name "%OdU_t 2"])))]
    (is (:products/id res))
    (is (not= 2 (:products/id res)))
    (is (not= "Product 2" (:products/name res)))))

(deftest it-finds-a-product-matching-with-similar-to
  (let [res (select! *env* (-> *env* :products (r/where [:similar-to :name "(P[rod]+uct 2|%duct 3)"])))]
    (is (= 2 (count res)))
    (is (= #{2 3} (->> res (map :products/id) set)))))

(deftest it-finds-a-product-matching-with-not-similar-to
  (let [res (select! *env* (-> *env* :products (r/where [:not-similar-to :name "(P[rod]+uct 2|%duct 3)"])))]
    (is (= 2 (count res)))
    (is (= #{1 4} (->> res (map :products/id) set)))))

(deftest it-finds-a-product-matching-a-case-sensitive-posix-regex
  (let [res (select-one! *env* (-> *env* :products (r/where ["~" :name "Product[ ]*1(?!otherstuff)"])))]
    (is (= 1 (:products/id res)))
    (is (= "Product 1" (:products/name res)))))

(deftest it-finds-a-product-not-matching-a-case-sensitive-posix-regex
  (let [res (select-one! *env* (-> *env* :products (r/where ["!~" :name "Product[ ]*[2-4](?!otherstuff)"])))]
    (is (= 1 (:products/id res)))
    (is (= "Product 1" (:products/name res)))))

(deftest it-finds-a-product-matching-a-case-insensitive-posix-regex
  (let [res (select-one! *env* (-> *env* :products (r/where ["~*" :name "product[ ]*1(?!otherstuff)"])))]
    (is (= 1 (:products/id res)))
    (is (= "Product 1" (:products/name res)))))

(deftest it-finds-a-product-not-matching-a-case-insensitive-posix-regex
  (let [res (select-one! *env* (-> *env* :products (r/where ["!~*" :name "product[ ]*[2-4](?!otherstuff)"])))]
    (is (= 1 (:products/id res)))
    (is (= "Product 1" (:products/name res)))))

(deftest it-finds-a-product-matching-the-desired-spec-field-in-json
  (let [res (select-one! *env* (-> *env* :products (r/where [:= ["#>>" :specs ["weight"]] [:cast 30 "text"]])))]
    (is (= 3 (:products/id res)))
    (is (= 30 (get-in res [:products/specs :weight])))))

(deftest it-finds-a-product-matching-the-desired-spec-index-in-json
  (let [res (select-one! *env* (-> *env* :products (r/where [:= ["->>" :specs (int 4)] "array"])))]
    (is (= 4 (:products/id res)))
    (is (= "array" (get-in res [:products/specs 4])))))

(deftest it-finds-a-product-matching-the-desired-spec-path-in-json
  (let [res (select-one! *env* (-> *env* :products (r/where [:= ["#>>" :specs ["dimensions" "length"]] "15"])))]
    (is (= 2 (:products/id res)))
    (is (= 15 (get-in res [:products/specs :dimensions :length])))))

(deftest it-finds-a-product-matching-the-desired-spec-in-an-in-list
  (let [res (select-one! *env* (-> *env* :products (r/where [:in ["#>>" :specs ["weight"]] ["30" "35"]])))]
    (is (= 3 (:products/id res)))
    (is (= 30 (get-in res [:products/specs :weight])))))

(deftest it-mixes-json-and-non-json-predicates
  (let [res (select-one! *env* (-> *env* :products (r/where [:and [:= :price 35.0] [:= ["#>>" :specs ["weight"]] "30"]])))]
    (is (= 3 (:products/id res)))
    (is (= 30 (get-in res [:products/specs :weight])))))

(deftest it-filters-by-array-fields-containing-a-value
  (let [res (select! *env* (-> *env* :products (r/where ["@>" :tags ["tag2"]])))]
    (is (= 2 (count res)))
    (is (= #{2 3} (->> res (map :products/id) set)))))

(deftest it-filters-by-array-fields-contained-in-a-value
  (let [res (select! *env* (-> *env* :products (r/where ["<@" :tags ["tag2" "tag3" "tag4"]])))]
    (is (= 1 (count res)))
    (is (= #{3} (->> res (map :products/id) set)))))

(deftest it-filters-by-array-fields-overlapping-a-value
  (let [res (select! *env* (-> *env* :products (r/where ["&&" :tags ["tag3" "tag4" "tag5"]])))]
    (is (= 2 (count res)))
    (is (= #{3 4} (->> res (map :products/id) set)))))

(deftest it-returns-1-result-with-limit-1
  (let [res (select! *env* (-> *env* :products (r/limit 1)))]
    (is (= 1 (count res)))))

(deftest it-returns-second-result-with-limit-1-offset-1
  (let [res (select! *env* (-> *env* :products (r/limit 1) (r/offset 1) (r/order-by [:id])))]
    (is (= 1 (count res)))
    (is (= 2 (-> res first :products/id)))))

(deftest it-restricts-the-select-list-to-specified-columns
  (let [res (select-one! *env* (-> *env* :products (r/select [:id :name])))]
    (is (= #{:products/id :products/name} (set (keys res))))))

(deftest it-allows-extending-columns
  (let [res (select-one! *env* (-> *env* :products
                                   (r/extend :upper-name [:upper :name])
                                   (r/select [:id :upper-name])
                                   (r/where [:= :id 1])))]
    (is (= {:products/id 1 :products/upper-name "PRODUCT 1"} res))))

(deftest it-returns-ascending-order-of-products-by-price
  (let [res (select! *env* (-> *env* :products (r/order-by [:price])))]
    (is (= 1 (get-in res [0 :products/id])))
    (is (= 3 (get-in res [2 :products/id])))))

(deftest it-returns-descending-order-of-products-by-price
  (let [res (select! *env* (-> *env* :products (r/order-by [[:price :desc]])))]
    (is (= 4 (get-in res [0 :products/id])))
    (is (= 2 (get-in res [2 :products/id])))))

(deftest it-changes-null-positioning
  (let [res (select! *env* (-> *env* :products (r/order-by [[:specs :asc :nulls-first]])))]
    (is (= 4 (count res)))
    (is (nil? (get-in res [0 :specs])))))

(deftest it-can-query-tables-with-weird-casing
  (let [res (select! *env* (-> *env* :users (r/where [:= :email "test@test.com"])))]
    (is (= 1 (count res)))))

(deftest it-can-use-aggregates
  (let [res (select-one! *env* (-> *env* :products
                                   (r/where [:in :price [12.00 24.00]])
                                   (r/extend-with-aggregate :count [:count 1])
                                   (r/select [:count])))]
    (is (= 2 (:products/count res)))))

(deftest it-can-use-aggregates-with-filter
  (let [res (select! *env* (-> *env* :products
                               (r/where [:in :price [12.00 24.00]])
                               (r/extend-with-aggregate :has-products-priced-12 [:> [:filter [:count 1] [:= :price 12.00]] 0])
                               (r/select [:id :has-products-priced-12])))]
    (is (= [{:products/has-products-priced-12 true, :products/id 1}
            {:products/has-products-priced-12 false, :products/id 2}]
           res))))

(deftest it-can-use-window-functions
  (let [res (select! *env* (-> *env* :products
                               (r/extend-with-window :sum-so-far [:sum :price])
                               (r/select [:id :price :sum-so-far])
                               (r/order-by [:id])))]
    (is (= [{:products/id 1 :products/price 12.00M :products/sum-so-far 111.00M}
            {:products/id 2 :products/price 24.00M :products/sum-so-far 111.00M}
            {:products/id 3 :products/price 35.00M :products/sum-so-far 111.00M}
            {:products/id 4 :products/price 40.00M :products/sum-so-far 111.00M}]
           res)))
  (let [res (select! *env* (-> *env* :products
                               (r/extend-with-window :sum-so-far [:sum :price] [:id])
                               (r/select [:id :price :sum-so-far])
                               (r/order-by [:id])))]
    (is (= [{:products/id 1 :products/price 12.00M :products/sum-so-far 12.00M}
            {:products/id 2 :products/price 24.00M :products/sum-so-far 24.00M}
            {:products/id 3 :products/price 35.00M :products/sum-so-far 35.00M}
            {:products/id 4 :products/price 40.00M :products/sum-so-far 40.00M}]
           res)))
  (let [res (select! *env* (-> *env* :products
                               (r/extend-with-window :sum-so-far [:sum :price] nil [:id])
                               (r/select [:id :price :sum-so-far])
                               (r/order-by [:id])))]
    (is (= [{:products/id 1 :products/price 12.00M :products/sum-so-far 12.00M}
            {:products/id 2 :products/price 24.00M :products/sum-so-far 36.00M}
            {:products/id 3 :products/price 35.00M :products/sum-so-far 71.00M}
            {:products/id 4 :products/price 40.00M :products/sum-so-far 111.00M}]
           res))))

(deftest it-can-query-views
  (let [res (select! *env* :popular-products)]
    (is (= 3 (count res)))))

(deftest it-can-restrict-columns-when-querying-views
  (let [res (select-one! *env* (-> *env* :popular-products
                                   (r/select [:id :price])))]
    (is (= #{:popular-products/id :popular-products/price} (set (keys res))))))

(deftest it-can-override-namespace-in-decomposition
  (let [res (select-one! *env* (-> *env* :popular-products
                                   (r/select [:id :price])) {} {:namespace :products})]
    (is (= #{:products/id :products/price} (set (keys res))))))

(deftest it-can-apply-predicates-when-querying-views
  (let [res (select! *env* (-> *env* :popular-products (r/where [:> :price 30.0])))]
    (is (= 1 (count res)))))

(deftest it-can-query-materialized-views
  (let [res (select! *env* :mv-orders)]
    (is (= 3 (count res)))))

(deftest it-can-use-case-1
  (let [products (-> *env*
                     :products
                     (r/extend :extended-col [:case :id
                                              [:when 1 "One"]
                                              [:when 2 "Two"]
                                              "neither one or two"])
                     (r/select [:id :extended-col]))
        res (select! *env* products)]

    (is (= [{:products/extended-col "One", :products/id 1}
            {:products/extended-col "Two", :products/id 2}
            {:products/extended-col "neither one or two", :products/id 3}
            {:products/extended-col "neither one or two", :products/id 4}]
           res))))

(deftest it-can-use-case-2
  (let [products (-> *env*
                     :products
                     (r/extend :extended-col [:case :id
                                              [:when 1 "One"]
                                              [:when 2 "Two"]])
                     (r/select [:id :extended-col]))
        res (select! *env* products)]

    (is (= [{:products/extended-col "One", :products/id 1}
            {:products/extended-col "Two", :products/id 2}
            {:products/extended-col nil, :products/id 3}
            {:products/extended-col nil, :products/id 4}]
           res))))

(deftest it-can-use-case-3
  (let [products (-> *env*
                     :products
                     (r/extend :extended-col [:case
                                              [:when [:= :id 1] "One"]
                                              [:when [:= :id 2] "Two"]
                                              "neither one or two"])
                     (r/select [:id :extended-col]))
        res (select! *env* products)]

    (is (= [{:products/extended-col "One", :products/id 1}
            {:products/extended-col "Two", :products/id 2}
            {:products/extended-col "neither one or two", :products/id 3}
            {:products/extended-col "neither one or two", :products/id 4}]
           res))))

(deftest it-can-use-case-4
  (let [products (-> *env*
                     :products
                     (r/extend :extended-col [:case
                                              [:when [:= :id 1] "One"]
                                              [:when [:= :id 2] "Two"]])
                     (r/select [:id :extended-col]))
        res (select! *env* products)]

    (is (= [{:products/extended-col "One", :products/id 1}
            {:products/extended-col "Two", :products/id 2}
            {:products/extended-col nil, :products/id 3}
            {:products/extended-col nil, :products/id 4}]
           res))))

(deftest it-can-use-case-inside-other-expression
  (let [products (-> *env*
                     :products
                     (r/extend :extended-col [:to-json [:case
                                                        [:when [:= :id 1] "One"]
                                                        [:when [:= :id 2] "Two"]]])
                     (r/select [:id :extended-col]))
        res (select! *env* products)]

    (is (= [{:products/extended-col "One", :products/id 1}
            {:products/extended-col "Two", :products/id 2}
            {:products/extended-col nil, :products/id 3}
            {:products/extended-col nil, :products/id 4}]
           res))))

(deftest it-will-throw-if-keyword-cant-match-a-column
  (is (thrown? clojure.lang.ExceptionInfo (-> (:products *env*) (r/where [:and [:= :id 2] [:= :name (keyword "Product 2")]])))))

(deftest it-can-use-union-as-operator
  (let [products (-> *env*
                     :products
                     (r/select [:id]))
        query (-> products
                  (r/where [:in :id [:union
                                     (-> products (r/where [:= :id 1]))
                                     (-> products (r/where [:= :id 2]))]]))
        res (select! *env* query)]

    (is (= #{1 2} (->> res (map :products/id) set)))))

(deftest it-can-use-except-as-operator
  (let [products (-> *env*
                     :products
                     (r/select [:id]))
        query (-> products
                  (r/where [:in :id [:except
                                     products
                                     (-> products (r/where [:= :id 2]))]]))
        res (select! *env* query)]

    (is (= #{1 3 4} (->> res (map :products/id) set)))))

(deftest it-can-use-intersect-as-operator
  (let [products (-> *env*
                     :products
                     (r/select [:id]))
        query (-> products
                  (r/where [:in :id [:intersect
                                     products
                                     (-> products (r/where [:in :id [1 2 3]]))
                                     (-> products (r/where [:in :id [2 3]]))]]))
        res (select! *env* query)]

    (is (= #{2 3} (->> res (map :products/id) set)))))

(deftest it-can-use-union-all-as-operator
  (let [products (-> *env*
                     :products
                     (r/select [:id]))
        query (-> products
                  (r/where [:in :id [:union-all
                                     (-> products (r/where [:= :id 1]))
                                     (-> products (r/where [:= :id 2]))]]))
        res (select! *env* query)]

    (is (= #{1 2} (->> res (map :products/id) set)))))

(deftest it-can-use-except-all-as-operator
  (let [products (-> *env*
                     :products
                     (r/select [:id]))
        query (-> products
                  (r/where [:in :id [:except-all
                                     products
                                     (-> products (r/where [:= :id 2]))]]))
        res (select! *env* query)]

    (is (= #{1 3 4} (->> res (map :products/id) set)))))

(deftest it-can-use-intersect-all-as-operator
  (let [products (-> *env*
                     :products
                     (r/select [:id]))
        query (-> products
                  (r/where [:in :id [:intersect-all
                                     products
                                     (-> products (r/where [:in :id [1 2 3]]))
                                     (-> products (r/where [:in :id [2 3]]))]]))
        res (select! *env* query)]

    (is (= #{2 3} (->> res (map :products/id) set)))))

(deftest it-can-use-union-intersect-except-together
  (let [products (-> *env*
                     :products
                     (r/select [:id]))
        query (-> products
                  (r/where [:in :id
                            [:except
                             [:union
                              (-> products (r/where [:in :id [1 2 4]]))
                              [:intersect
                               (-> products (r/where [:in :id [1 2]]))
                               (-> products (r/where [:in :id [2 3]]))]]
                             (-> products (r/where [:= :id 4]))]]))
        res (select! *env* query)]

    (is (= #{1 2} (->> res (map :products/id) set)))))