(ns com.verybigthings.penkala.statement.select-test
  (:require [clojure.test :refer :all]
            [com.verybigthings.penkala.relation :as r]))

(def rel
  (r/spec->relation {:columns ["id" "name"]
                     :name "foo"
                     :pk ["id"]}))

(deftest it-adds-distinct
  (let [[q & _] (r/get-select-query (r/distinct rel) {})]
    (is (= "SELECT DISTINCT \"foo\".\"id\" AS \"id\", \"foo\".\"name\" AS \"name\" FROM \"foo\" AS \"foo\"" q))))

(deftest it-adds-distinct-on
  (let [[q & _] (r/get-select-query (r/distinct rel [:id]) {})]
    (is (= "SELECT DISTINCT ON(\"foo\".\"id\") \"foo\".\"id\" AS \"id\", \"foo\".\"name\" AS \"name\" FROM \"foo\" AS \"foo\"" q))))

(deftest it-adds-locks
  (let [[q & _] (r/get-select-query (r/lock rel :share) {})]
    (is (= "SELECT \"foo\".\"id\" AS \"id\", \"foo\".\"name\" AS \"name\" FROM \"foo\" AS \"foo\" FOR SHARE" q)))
  (let [[q & _] (r/get-select-query (r/lock rel :update) {})]
    (is (= "SELECT \"foo\".\"id\" AS \"id\", \"foo\".\"name\" AS \"name\" FROM \"foo\" AS \"foo\" FOR UPDATE" q)))
  (let [[q & _] (r/get-select-query (r/lock rel :no-key-update) {})]
    (is (= "SELECT \"foo\".\"id\" AS \"id\", \"foo\".\"name\" AS \"name\" FROM \"foo\" AS \"foo\" FOR NO KEY UPDATE" q)))
  (let [[q & _] (r/get-select-query (r/lock rel :key-share) {})]
    (is (= "SELECT \"foo\".\"id\" AS \"id\", \"foo\".\"name\" AS \"name\" FROM \"foo\" AS \"foo\" FOR KEY SHARE" q)))
  (let [[q & _] (r/get-select-query (r/lock rel :share :nowait) {})]
    (is (= "SELECT \"foo\".\"id\" AS \"id\", \"foo\".\"name\" AS \"name\" FROM \"foo\" AS \"foo\" FOR SHARE NOWAIT" q)))
  (let [[q & _] (r/get-select-query (r/lock rel :share :skip-locked) {})]
    (is (= "SELECT \"foo\".\"id\" AS \"id\", \"foo\".\"name\" AS \"name\" FROM \"foo\" AS \"foo\" FOR SHARE SKIP LOCKED" q))))

(deftest it-adds-group-by-when-selecting-aggregate
  (let [[q & _] (-> rel
                  (r/extend-with-aggregate :count [:count (r/l 1)])
                  (r/get-select-query {}))]
    (is (= "SELECT count(1) AS \"count\", \"foo\".\"id\" AS \"id\", \"foo\".\"name\" AS \"name\" FROM \"foo\" AS \"foo\" GROUP BY \"foo\".\"id\", \"foo\".\"name\""
          q))))

(deftest it-adds-having-when-selecting-aggregate
  (let [[q & _] (-> rel
                  (r/extend-with-aggregate :count [:count (r/l 1)])
                  (r/having [:> :count (r/l 1)])
                  (r/get-select-query {}))]
    (is (= "SELECT count(1) AS \"count\", \"foo\".\"id\" AS \"id\", \"foo\".\"name\" AS \"name\" FROM \"foo\" AS \"foo\" GROUP BY \"foo\".\"id\", \"foo\".\"name\" HAVING count(1) > 1"
          q))))
