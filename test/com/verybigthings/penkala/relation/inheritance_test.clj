(ns com.verybigthings.penkala.relation.inheritance-test
  (:require [clojure.test :refer [use-fixtures deftest is]]
            [com.verybigthings.penkala.next-jdbc :refer [select!]]
            [com.verybigthings.penkala.relation :as r]
            [com.verybigthings.penkala.test-helpers :as th :refer [*env*]]))

(use-fixtures :once (partial th/reset-db-fixture "inheritance"))

(deftest it-can-limit-inheritance-with-only
  (let [cities (:cities *env*)
        capitals (:capitals *env*)]
    (is (= [{:cities/id 1, :cities/name "Oklahoma City", :cities/population 631346}
            {:cities/id 2, :cities/name "Phoenix", :cities/population 1563025}
            {:cities/id 3, :cities/name "Nashville", :cities/population 654610}
            {:cities/id 4, :cities/name "Indianapolis", :cities/population 853173}
            {:cities/id 5, :cities/name "San Antonio", :cities/population 1469845}
            {:cities/id 6, :cities/name "Chesapeake", :cities/population 235429}]
           (select! *env* cities)))
    (is (= [{:cities/id 1, :cities/name "Anchorage", :cities/population 298615}
            {:cities/id 2, :cities/name "Jacksonville", :cities/population 868031}
            {:cities/id 3, :cities/name "Houston", :cities/population 2296224}
            {:cities/id 4, :cities/name "Los Angeles", :cities/population 3971883}
            {:cities/id 5, :cities/name "San Antonio", :cities/population 1469845}
            {:cities/id 6, :cities/name "Chesapeake", :cities/population 235429}]
           (select! *env* (r/only cities))))
    (is (= [{:capitals/id 1
             :capitals/name "Oklahoma City"
             :capitals/of-state "OK"
             :capitals/population 631346}
            {:capitals/id 2
             :capitals/name "Phoenix"
             :capitals/of-state "AZ"
             :capitals/population 1563025}
            {:capitals/id 3
             :capitals/name "Nashville"
             :capitals/of-state "TN"
             :capitals/population 654610}
            {:capitals/id 4
             :capitals/name "Indianapolis"
             :capitals/of-state "IN"
             :capitals/population 853173}]
           (select! *env* capitals)))))