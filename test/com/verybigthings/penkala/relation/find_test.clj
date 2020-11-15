(ns com.verybigthings.penkala.relation.find-test
  (:require [clojure.test :refer :all]
            [com.verybigthings.penkala.db :refer [select! select-one!]]
            [com.verybigthings.penkala.relation :as r]
            [com.verybigthings.penkala.test-helpers :as th :refer [db-uri *env*]]
            [com.verybigthings.penkala.env :as env]
            [next.jdbc :as jdbc]
            [clojure.spec.alpha :as s]))

(s/check-asserts true)

(use-fixtures :once (partial th/reset-db-fixture "data-all"))

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
  (let [res (select! *env* (-> *env* :products (r/where [:is-distinct-from :tags (r/ql "{tag1,tag2}")])))
        res2 (select! *env* (-> *env* :products (r/where [:is-distinct-from :tags (r/array-literal ["tag1" "tag2"])])))
        res3 (select! *env* (-> *env* :products (r/where [:is-distinct-from :tags ["tag1" "tag2"]])))]
    (is (= 3 (count res) (count res2) (count res3)))
    (is (= #{1 3 4}
          (->> res (map :products/id) set)
          (->> res2 (map :products/id) set)
          (->> res3 (map :products/id) set)))))

(deftest it-returns-products-using-not-distinct-from
  (let [res (select! *env* (-> *env* :products (r/where [:is-not-distinct-from :tags (r/ql "{tag1,tag2}")])))
        res2 (select! *env* (-> *env* :products (r/where [:is-not-distinct-from :tags (r/array-literal ["tag1" "tag2"])])))
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

#_(def db-spec
    [{:schema "public",
      :is_insertable_into true,
      :fk_origin_columns nil,
      :pk ["id"],
      :parent nil,
      :columns ["body" "id" "search"],
      :name "uuid_docs",
      :fk_dependent_columns nil,
      :fk_origin_schema nil,
      :fk_origin_name nil,
      :fk nil}
     {:schema "public",
      :is_insertable_into true,
      :fk_origin_columns nil,
      :pk ["id"],
      :parent nil,
      :columns
      ["created_at"
       "description"
       "id"
       "in_stock"
       "name"
       "price"
       "specs"
       "tags"],
      :name "products",
      :fk_dependent_columns nil,
      :fk_origin_schema nil,
      :fk_origin_name nil,
      :fk nil}
     {:schema "public",
      :is_insertable_into true,
      :fk_origin_columns nil,
      :pk ["id"],
      :parent nil,
      :columns ["id" "notes" "ordered_at" "product_id" "user_id"],
      :name "orders",
      :fk_dependent_columns nil,
      :fk_origin_schema nil,
      :fk_origin_name nil,
      :fk nil}
     {:schema "public",
      :is_insertable_into true,
      :fk_origin_columns nil,
      :pk ["id"],
      :parent nil,
      :columns ["body" "id" "search"],
      :name "docs",
      :fk_dependent_columns nil,
      :fk_origin_schema nil,
      :fk_origin_name nil,
      :fk nil}
     {:schema "public",
      :is_insertable_into true,
      :fk_origin_columns nil,
      :pk ["Id"],
      :parent nil,
      :columns ["Email" "Id" "Name" "search"],
      :name "Users",
      :fk_dependent_columns nil,
      :fk_origin_schema nil,
      :fk_origin_name nil,
      :fk nil}])

#_(deftest testing1
    (let [products      (rel2/spec->relation {:schema "public",
                                              :is_insertable_into true,
                                              :fk_origin_columns nil,
                                              :pk ["id"],
                                              :parent nil,
                                              :columns ["created_at" "description" "id" "in_stock" "name" "price" "specs" "tags"],
                                              :name "products",
                                              :fk_dependent_columns nil,
                                              :fk_origin_schema nil,
                                              :fk_origin_name nil,
                                              :fk nil})
          orders        (rel2/spec->relation {:schema "public",
                                              :is_insertable_into true,
                                              :fk_origin_columns nil,
                                              :pk ["id"],
                                              :parent nil,
                                              :columns ["id" "notes" "ordered_at" "product_id" "user_id"],
                                              :name "orders",
                                              :fk_dependent_columns nil,
                                              :fk_origin_schema nil,
                                              :fk_origin_name nil,
                                              :fk nil})
          users         (rel2/spec->relation {:schema "public",
                                              :is_insertable_into true,
                                              :fk_origin_columns nil,
                                              :pk ["Id"],
                                              :parent nil,
                                              :columns ["Email" "Id" "Name" "search"],
                                              :name "Users",
                                              :fk_dependent_columns nil,
                                              :fk_origin_schema nil,
                                              :fk_origin_name nil,
                                              :fk nil})
          computed-rel  (rel2/spec->relation {:columns ["foo" "bar"]
                                              :name (str (gensym "rel_"))
                                              :query ["SELECT foo, bar FROM (VALUES ('foo1', 'bar1'), ('foo2', 'bar2')) AS q (foo, bar)"]})
          computed-rel2 (rel2/spec->relation {:columns ["foo" "qux"]
                                              :name (str (gensym "rel_"))
                                              :query ["SELECT foo, qux FROM (VALUES ('foo1', 'qux1'), ('foo2', 'qux2')) AS q (foo, qux)"]})
          rel           (-> products
                          ;;(rel2/extend-with-window :window-sum [:sum :id] nil [:id])
                          (rel2/rename :name :foo)
                          (rel2/lock :share)
                          ;;(rel2/where [:parent-scope [:and [:= :id 1] [:= :id 2]]])
                          ;;(rel2/where [:= :id 1])
                          #_(rel2/extend-with-aggregate :count-products :count 1)
                          ;;(rel2/select [:id :count-products])
                          ;;(rel2/only)
                          ;;(rel2/distinct [:name (rel2/column :id)])
                          ;;(rel2/distinct false)
                          #_(rel2/join
                              :left
                              (rel2/join orders :left
                                (-> users
                                  (rel2/alias :foo-bar-users)
                                  (rel2/extend :upper-name [:upper :name])
                                  (rel2/extend-with-aggregate :count :count 1)
                                  (rel2/extend :lower-name [:lower :upper-name])
                                  (rel2/rename :lower-name :ln)
                                  (rel2/having [:< :count 1]))
                                :users [:= :user-id :users/id])
                              :orders
                              [:or true [:is-not-false [:not false]] [:foo :bar] [:and [:is-true true] [:= :id :orders/product-id]]])
                          #_(rel2/select [:id])

                          #_(rel2/where [:in :id (-> products
                                                   (rel2/select [:id])
                                                   (rel2/where [:= :id 1]))])
                          ;;(rel2/where [:= :orders.users/ln "A TEST USER"])

                          ;;(rel2/rename :name :product-name)
                          ;;(rel2/extend :upper-product-name [:upper :product-name])
                          ;;(rel2/rename :upper-product-name :upn)
                          ;;(rel2/extend :lpn [:lower :upn])
                          ;;(rel2/order-by [[:lpn :desc]])
                          ;;(rel2/select [:lpn])
                          ;;(rel2/where [:and [:= :upn (rel2/param :product-name)] [:= :id (rel2/param :product-id)]])
                          ;;(rel2/offset 1)
                          ;;(rel2/limit 2)
                          )
          #_#_p1 (-> products
                   (rel2/select [:id :name])
                   (rel2/join :left (rel2/select orders [:id :product-id]) :orders [:= :id :orders/product-id])
                   (rel2/where [:or [:= :id 1] [:= :id 2]]))
          #_#_p2 (-> products
                   (rel2/select [:id :name])
                   (rel2/join :left (rel2/select orders [:id :product-id]) :orders [:= :id :orders/product-id])
                   (rel2/where [:or [:= :id 2] [:= :id 3]]))
          #_#_p1-p2 (rel2/except p1 p2)
          #_#_pp (-> products
                   (rel2/join :left p1-p2 :pp [:= :id :pp/id]))
          p-with-parent (rel2/with-parent products products)
          ptest         (-> products
                          (rel2/extend :pname (-> p-with-parent
                                                (rel2/select [:name])
                                                (rel2/where [:= :id [:parent-scope :id]])))
                          (rel2/select [:id :pname]))

          ptest2        (-> products
                          (rel2/select [:id :name])
                          (rel2/join :inner-lateral
                            (-> orders
                              (rel2/with-parent products)
                              (rel2/extend-with-aggregate :serialized [:json-agg [:json-build-object (rel2/quoted-literal :id) :id, (rel2/quoted-literal :user-id), :user-id]])
                              (rel2/select [:serialized :product-id])
                              (rel2/where [:= :product-id [:parent-scope :id]]))
                            :orders
                            [:= :id :orders/product-id]))

          ptest3        (-> products
                          (rel2/where [:fragment (fn [env rel [[query1 & params1]]] [(str query1 " = 2") params1]) :id]))]
      ;;   (println (jdbc/execute! db-uri [(rel/to-sql rel)]))
      ;;(clojure.pprint/pprint ptest3)
      ;;(println (prettify-sql (first (sel/format-query {} (rel2/join orders :left users :users [:= :user-id :users/id]) {:product-name "PRODUCT 1" :product-id 1}))))
      ;;(println (sel/format-query {} (rel2/join computed-rel :inner computed-rel2 :c2 [:= :foo :c2/foo]) {}))
      (println (prettify-sql (first (sel/format-query {} rel {:product-name "PRODUCT 1" :product-id 1}))))
      (println (first (sel/format-query {} rel {:product-name "PRODUCT 1" :product-id 1})))
      ;;(println (-> (rel2/get-select-query p1-p2 {}) first        prettify-sql ))
      ;;(println (sel/format-query {} rel {:product-name "PRODUCT 1" :product-id 1}))
      ;;(println (jdbc/execute! db-uri (sel/format-query {} rel {})))
      (is false)))

#_(deftest testing1
    (let [products              (rel2/spec->relation {:schema "public",
                                                      :is_insertable_into true,
                                                      :fk_origin_columns nil,
                                                      :pk ["id"],
                                                      :parent nil,
                                                      :columns ["created_at" "description" "id" "in_stock" "name" "price" "specs" "tags"],
                                                      :name "products",
                                                      :fk_dependent_columns nil,
                                                      :fk_origin_schema nil,
                                                      :fk_origin_name nil,
                                                      :fk nil})
          orders                (rel2/spec->relation {:schema "public",
                                                      :is_insertable_into true,
                                                      :fk_origin_columns nil,
                                                      :pk ["id"],
                                                      :parent nil,
                                                      :columns ["id" "notes" "ordered_at" "product_id" "user_id"],
                                                      :name "orders",
                                                      :fk_dependent_columns nil,
                                                      :fk_origin_schema nil,
                                                      :fk_origin_name nil,
                                                      :fk nil})
          users                 (rel2/spec->relation {:schema "public",
                                                      :is_insertable_into true,
                                                      :fk_origin_columns nil,
                                                      :pk ["Id"],
                                                      :parent nil,
                                                      :columns ["Email" "Id" "Name" "search"],
                                                      :name "Users",
                                                      :fk_dependent_columns nil,
                                                      :fk_origin_schema nil,
                                                      :fk_origin_name nil,
                                                      :fk nil})
          orders-users          (rel2/join orders :left users :users [:= :user-id :users/id])
          products-orders-users (-> products (rel2/join :left orders-users :orders [:= :id :orders/product-id]))]
      (infer-schema products-orders-users)
      ;;(clojure.pprint/pprint (infer-schema products-orders-users))
      ;;(println (prettify-sql (first (sel/format-query {} products-orders-users {}))))
      ;;(println (first (sel/format-query {} products-orders-users {})))
      (decompose (infer-schema products-orders-users) (execute! db-uri (sel/format-query {} products-orders-users {})))
      (clojure.pprint/pprint (execute! db-uri (sel/format-query {} products-orders-users {})))
      (clojure.pprint/pprint (decompose (infer-schema products-orders-users) (execute! db-uri (sel/format-query {} products-orders-users {}))))
      (is false)))