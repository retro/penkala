(ns com.verybigthings.penkala.statement.operations-test
  (:require [clojure.test :refer :all]
            [com.verybigthings.penkala.statement.operations :as o]))

(deftest build-between
  (testing "it builds a BETWEEN predicate"
    (let [condition (o/build-between {:value [1 100] :params []})]
      (is (= {:params [1 100] :value "? AND ?"} condition))))

  (testing "it typecasts timestamps"
    (let [date1 (java.time.Instant/now)
          date2 (java.time.Instant/now)
          condition (o/build-between {:value [date1 date2] :params []})]
      (is (= {:params [date1 date2] :value "?::timestamptz AND ?::timestamptz"} condition)))))

(deftest build-in
  (testing "it builds an IN list"
    (let [condition (o/build-in {:appended (o/operations "=") :value [1 2 3] :params []})]
      (is (= "IN" (get-in condition [:appended :operator])))
      (is (= [1 2 3] (:params condition)))
      (is (= "(?,?,?)" (:value condition)))))

  (testing "it builds an NOT IN list"
    (let [condition (o/build-in {:appended (o/operations "<>") :value [1 2 3] :params []})]
      (is (= "NOT IN" (get-in condition [:appended :operator])))
      (is (= [1 2 3] (:params condition)))
      (is (= "(?,?,?)" (:value condition)))))

  (testing "it builds an IN list for an empty array"
    (let [condition (o/build-in {:appended (o/operations "=") :value [] :params []})]
      (is (= "=" (get-in condition [:appended :operator])))
      (is (= [] (:params condition)))
      (is (= "ANY ('{}')" (:value condition)))))

  (testing "it builds an NOT IN list for an empty array"
    (let [condition (o/build-in {:appended (o/operations "<>") :value [] :params []})]
      (is (= "<>" (get-in condition [:appended :operator])))
      (is (= [] (:params condition)))
      (is (= "ALL ('{}')" (:value condition)))))

  (testing "it typecasts timestamps"
    (let [date1 (java.time.Instant/now)
          date2 (java.time.Instant/now)
          condition (o/build-in {:appended (o/operations "=") :value [date1 date2] :params []})]
      (is (= "IN" (get-in condition [:appended :operator])))
      (is (= [date1 date2] (:params condition)))
      (is (= "(?::timestamptz,?::timestamptz)" (:value condition))))))

(deftest build-is
  (testing "it interpolates values with IS"
    (let [condition (o/build-is {:appended (o/operations "=") :value nil :params []})]
      (is (= "IS" (get-in condition [:appended :operator])))
      (is (= [] (:params condition)))
      (is (nil? (:value condition)))))

  (testing "it interpolates values with IS NOT"
    (let [condition (o/build-is {:appended (o/operations "is not") :value true :params []})]
      (is (= "IS NOT" (get-in condition [:appended :operator])))
      (is (= [] (:params condition)))
      (is (true? (:value condition))))))

(deftest equality
  (testing "it passes off arrays to build-in"
    (let [condition (o/equality {:appended (o/operations "=") :value [1 2 3] :params []})]
      (is (= "IN" (get-in condition [:appended :operator])))
      (is (= [1 2 3] (:params condition)))
      (is (= "(?,?,?)" (:value condition)))))

  (testing "it passes nulls and booleans to build-is"
    (let [c1 (o/equality {:appended (o/operations "is") :value nil :params []})
          c2 (o/equality {:appended (o/operations "=") :value true :params []})
          c3 (o/equality {:appended (o/operations "=") :value false :params []})]
      (is (= "IS"
            (get-in c1 [:appended :operator])
            (get-in c2 [:appended :operator])
            (get-in c3 [:appended :operator])))))

  (testing "it prepares parameters"
    (let [condition (o/equality {:appended (o/operations "=") :value 123 :params []})]
      (is (= "=" (get-in condition [:appended :operator])))
      (is (= [123] (:params condition)))
      (is (= "?" (:value condition)))))

  (testing "it typecasts timestamps"
    (let [date1 (java.time.Instant/now)
          condition (o/equality {:appended (o/operations "=") :value date1 :params []})]
      (is (= "=" (get-in condition [:appended :operator])))
      (is (= [date1] (:params condition)))
      (is (= "?::timestamptz" (:value condition))))))

(deftest literalize-array
  (testing "it transforms arrays into Postgres syntax"
    (let [condition (o/literalize-array {:value ["one" "two" "three"] :params []})]
      (is (= {:params ["{one,two,three}"] :value "?"} condition))))
  (testing "it leaves non-array values alone"
    (let [condition (o/literalize-array {:value "hi" :params []})]
      (is (= {:params ["hi"] :value "?"} condition))))
  (testing "it sanitizes string values"
    (let [condition (o/literalize-array {:value ["{one}" "two three" "four,five" "\"six\"" "\\seven" "" "null"]
                                         :params []})]
      (is (= {:params ["{\"{one}\",\"two three\",\"four,five\",\"\"six\"\",\"\\seven\",\"\",\"null\"}"]
              :value "?"}
            condition))))
  (testing "it does nothing to non-string values"
    (let [condition (o/literalize-array {:value [1, true, nil, 1.5] :params []})]
      (is (= {:params ["{1,true,null,1.5}"] :value "?"} condition)))))