(ns com.verybigthings.penkala.relation.join-test
  (:require [clojure.test :refer :all]
            [com.verybigthings.penkala.db :refer [query query-one]]
            [com.verybigthings.penkala.relation :as r]
            [com.verybigthings.penkala.test-helpers :as th :refer [db-uri *db*]]))

(use-fixtures :once (partial th/reset-db-fixture "foreign-keys"))

(deftest it-joins-a-relation-with-a-type-and-keys
  (let [alpha (:relation/alpha *db*)
        beta (:relation/beta *db*)
        alpha-beta (r/inner-join alpha :beta beta {:alpha_id :id})
        res (query db-uri *db* (r/where alpha-beta {"alpha.id" 3}))]
    (is (= [{:id 3
             :val "three"
             :beta [{:id 3 :alpha_id 3 :j nil :val "alpha three"}
                    {:id 4 :alpha_id 3 :j nil :val "alpha three again"}]}]
          res))))