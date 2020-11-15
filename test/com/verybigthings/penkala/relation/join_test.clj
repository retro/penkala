(ns com.verybigthings.penkala.relation.join-test
  (:require [clojure.test :refer :all]
            [com.verybigthings.penkala.db :refer [select!]]
            [com.verybigthings.penkala.relation :as r]
            [com.verybigthings.penkala.test-helpers :as th :refer [*env*]]))

(use-fixtures :once (partial th/reset-db-fixture "foreign-keys"))

#_(deftest it-joins-a-relation-with-a-type-and-keys
  (let [alpha (:relation/alpha *env*)
        beta (:relation/beta *env*)
        alpha-beta (r/inner-join alpha :beta beta {:alpha_id :id})
        res (query db-uri *env* (r/where alpha-beta {"alpha.id" 3}))]
    (is (= [{:id 3
             :val "three"
             :beta [{:id 3 :alpha_id 3 :j nil :val "alpha three"}
                    {:id 4 :alpha_id 3 :j nil :val "alpha three again"}]}]
          res))))

#_(deftest it-joins-multiple-tables-at-multiple-levels
  (let [alpha (:relation/alpha *env*)
        beta (:relation/beta *env*)
        gamma (:relation/gamma *env*)
        sch-delta (:sch.relation/delta *env*)
        sch-epsilon (:sch.relation/epsilon *env*)
        joined (-> alpha
                 (r/inner-join
                   :beta (-> beta
                           (r/inner-join :gamma gamma {:beta_id "beta.id"})
                           (r/inner-join :sch.delta sch-delta {:beta_id "beta.id"}))
                   {:alpha_id :id})
                 (r/inner-join :sch.epsilon sch-epsilon {:alpha_id :id}))
        res (query db-uri *env* (r/where joined {"alpha.id" 1}))]
    (is (= [{:beta [{:alpha_id 1,
                     :delta [{:beta_id 1, :id 1, :val "beta one"}],
                     :gamma [{:alpha_id_one 1,
                              :alpha_id_two 1,
                              :beta_id 1,
                              :id 1,
                              :j nil,
                              :val "alpha one alpha one beta one"}],
                     :id 1,
                     :j nil,
                     :val "alpha one"}],
             :epsilon [{:alpha_id 1, :id 1, :val "alpha one"}],
             :id 1,
             :val "one"}]
          res))))
