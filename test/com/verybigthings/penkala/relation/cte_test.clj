(ns com.verybigthings.penkala.relation.cte-test
  (:require [clojure.test :refer [use-fixtures deftest is]]
            [com.verybigthings.penkala.next-jdbc :refer [select! select-one! insert!]]
            [com.verybigthings.penkala.relation :as r]
            [com.verybigthings.penkala.helpers :as h]
            [com.verybigthings.penkala.env :as env]
            [com.verybigthings.penkala.test-helpers :as th :refer [*env*]]
            [next.jdbc :as jdbc]))

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

(deftest insertable-cte
  (jdbc/with-transaction [tx (env/get-db *env*)]
    (let [env (env/with-db *env* tx)
          insertable-users (r/as-cte
                            (-> (:users env)
                                r/->insertable
                                (r/with-inserts {:email "example@example.com" :name "test user"})))
          query (-> insertable-users
                    (r/select [:email :name]))
          res (select! env query)]
      (is (= [{:users/email "example@example.com", :users/name "test user"}] res))
      (.rollback tx))))

(deftest insertable-cte
  (jdbc/with-transaction [tx (env/get-db *env*)]
    (let [env (env/with-db *env* tx)
          insertable-users (r/as-cte
                            (-> (:users env)
                                r/->insertable
                                (r/with-inserts [{:email "example1@example.com" :name "test user1"}
                                                 {:email "example2@example.com" :name "test user2"}])))
          query (-> insertable-users
                    (r/select [:email :name]))
          res (select! env query)]
      (is (= [{:users/email "example1@example.com", :users/name "test user1"}
              {:users/email "example2@example.com", :users/name "test user2"}] res))
      (.rollback tx))))

(deftest insertable-cte-with-where-on-select
  (jdbc/with-transaction [tx (env/get-db *env*)]
    (let [env (env/with-db *env* tx)
          insertable-users (r/as-cte
                            (-> (:users env)
                                r/->insertable
                                (r/with-inserts [{:email "example1@example.com" :name "test user1"}
                                                 {:email "example2@example.com" :name "test user2"}])))
          query (-> insertable-users
                    (r/select [:email :name])
                    (r/where [:= :email "example1@example.com"]))
          res (select! env query)]
      (is (= [{:users/email "example1@example.com", :users/name "test user1"}] res))
      (.rollback tx))))

(deftest updatable-cte
  (jdbc/with-transaction [tx (env/get-db *env*)]
    (let [env (env/with-db *env* tx)
          _ (insert! env (-> env :users r/->insertable) [{:email "example1@example.com" :name "test user1"}
                                                         {:email "example2@example.com" :name "test user2"}])
          updatable-users (r/as-cte
                           (-> (:users env)
                               r/->updatable
                               (r/where [:= :email "example1@example.com"])
                               (r/with-updates {:email "example1-updated@example.com"})))
          query (-> updatable-users
                    (r/select [:email :name]))
          res (select! env query)]
      (is (= [{:users/email "example1-updated@example.com", :users/name "test user1"}] res))
      (.rollback tx))))

(deftest deletable-cte
  (jdbc/with-transaction [tx (env/get-db *env*)]
    (let [env (env/with-db *env* tx)
          _ (insert! env (-> env :users r/->insertable) [{:email "example1@example.com" :name "test user1"}
                                                         {:email "example2@example.com" :name "test user2"}])
          deletable-users (r/as-cte
                           (-> (:users env)
                               r/->deletable
                               (r/where [:= :email "example1@example.com"])))
          query (-> deletable-users
                    (r/select [:email :name]))
          res (select! env query)]
      (is (= [{:users/email "example1@example.com", :users/name "test user1"}] res))
      (.rollback tx))))

(deftest converting-insertable-updatable-or-deletable-intorecursive-cte-throws
  (is (thrown? Exception (r/as-cte
                          (-> *env* :users r/->insertable)
                          (union [_]
                                 (-> *env* :users)))))
  (is (thrown? Exception (r/as-cte
                          (-> *env* :users r/->updatable)
                          (union [_]
                                 (-> *env* :users)))))
  (is (thrown? Exception (r/as-cte
                          (-> *env* :users r/->deletable)
                          (union [_]
                                 (-> *env* :users))))))