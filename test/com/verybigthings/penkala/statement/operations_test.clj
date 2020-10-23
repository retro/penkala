(ns com.verybigthings.penkala.statement.operations-test
  (:require [clojure.test :refer :all]
            [com.verybigthings.penkala.statement.operations :as o]))

(deftest build-between
  (testing "it builds a BETWEEN predicate"
    (let [condition (o/build-between {:offset 1 :value [1 100] :params []})]
      (is (= {:offset 3 :params [1 100] :value "$1 AND $2"} condition))))

  (testing "it typecasts timestamps"
    (let [date1 (java.time.Instant/now)
          date2 (java.time.Instant/now)
          condition (o/build-between {:offset 1 :value [date1 date2] :params []})]
      (is (= {:offset 3 :params [date1 date2] :value "$1::timestamptz AND $2::timestamptz"} condition)))))

(deftest build-in
  (testing "it builds an IN list"
    (let [condition (o/build-in {:appended (o/operations "=") :offset 1 :value [1 2 3] :params []})]
      (is (= "IN" (get-in condition [:appended :operator])))
      (is (= 4 (:offset condition)))
      (is (= [1 2 3] (:params condition)))
      (is (= "($1,$2,$3)" (:value condition)))))

  (testing "it builds an NOT IN list"
    (let [condition (o/build-in {:appended (o/operations "<>") :offset 1 :value [1 2 3] :params []})]
      (is (= "NOT IN" (get-in condition [:appended :operator])))
      (is (= 4 (:offset condition)))
      (is (= [1 2 3] (:params condition)))
      (is (= "($1,$2,$3)" (:value condition)))))

  (testing "it builds an IN list for an empty array"
    (let [condition (o/build-in {:appended (o/operations "=") :offset 1 :value [] :params []})]
      (is (= "=" (get-in condition [:appended :operator])))
      (is (= 1 (:offset condition)))
      (is (= [] (:params condition)))
      (is (= "ANY ('{}')" (:value condition)))))

  (testing "it builds an NOT IN list for an empty array"
    (let [condition (o/build-in {:appended (o/operations "<>") :offset 1 :value [] :params []})]
      (is (= "<>" (get-in condition [:appended :operator])))
      (is (= 1 (:offset condition)))
      (is (= [] (:params condition)))
      (is (= "ALL ('{}')" (:value condition)))))

  (testing "it typecasts timestamps"
    (let [date1 (java.time.Instant/now)
          date2 (java.time.Instant/now)
          condition (o/build-in {:appended (o/operations "=") :offset 1 :value [date1 date2] :params []})]
      (is (= "IN" (get-in condition [:appended :operator])))
      (is (= 3 (:offset condition)))
      (is (= [date1 date2] (:params condition)))
      (is (= "($1::timestamptz,$2::timestamptz)" (:value condition))))))

(deftest build-is
  (testing "it interpolates values with IS"
    (let [condition (o/build-is {:appended (o/operations "=") :offset 1 :value nil :params []})]
      (is (= "IS" (get-in condition [:appended :operator])))
      (is (= 1 (:offset condition)))
      (is (= [] (:params condition)))
      (is (nil? (:value condition)))))

  (testing "it interpolates values with IS NOT"
    (let [condition (o/build-is {:appended (o/operations "is not") :offset 1 :value true :params []})]
      (is (= "IS NOT" (get-in condition [:appended :operator])))
      (is (= 1 (:offset condition)))
      (is (= [] (:params condition)))
      (is (true? (:value condition))))))

(deftest equality
  (testing "it passes off arrays to build-in"
    (let [condition (o/equality {:appended (o/operations "=") :offset 1 :value [1 2 3] :params []})]
      (is (= "IN" (get-in condition [:appended :operator])))
      (is (= 4 (:offset condition)))
      (is (= [1 2 3] (:params condition)))
      (is (= "($1,$2,$3)" (:value condition)))))

  (testing "it passes nulls and booleans to build-is"
    (let [c1 (o/equality {:appended (o/operations "is") :offset 1 :value nil :params []})
          c2 (o/equality {:appended (o/operations "=") :offset 1 :value true :params []})
          c3 (o/equality {:appended (o/operations "=") :offset 1 :value false :params []})]
      (is (= "IS"
            (get-in c1 [:appended :operator])
            (get-in c2 [:appended :operator])
            (get-in c3 [:appended :operator])))))

  (testing "it prepares parameters"
    (let [condition (o/equality {:appended (o/operations "=") :offset 1 :value 123 :params []})]
      (is (= "=" (get-in condition [:appended :operator])))
      (is (= 1 (:offset condition)))
      (is (= [123] (:params condition)))
      (is (= "$1" (:value condition)))))

  (testing "it typecasts timestamps"
    (let [date1 (java.time.Instant/now)
          condition (o/equality {:appended (o/operations "=") :offset 1 :value date1 :params []})]
      (is (= "=" (get-in condition [:appended :operator])))
      (is (= 1 (:offset condition)))
      (is (= [date1] (:params condition)))
      (is (= "$1::timestamptz" (:value condition))))))

(deftest literalize-array
  (testing "it transforms arrays into Postgres syntax"
    (let [condition (o/literalize-array {:offset 1 :value ["one" "two" "three"] :params []})]
      (is (= {:offset 1 :params ["{one,two,three}"] :value "$1"} condition))))
  (testing "it leaves non-array values alone"
    (let [condition (o/literalize-array {:offset 1 :value "hi" :params []})]
      (is (= {:offset 1 :params ["hi"] :value "$1"} condition))))
  (testing "it sanitizes string values"
    (let [condition (o/literalize-array {:offset 1
                                         :value ["{one}" "two three" "four,five" "\"six\"" "\\seven" "" "null"]
                                         :params []})]
      (is (= {:offset 1
              :params ["{\"{one}\",\"two three\",\"four,five\",\"\"six\"\",\"\\seven\",\"\",\"null\"}"]
              :value "$1"}
            condition))))
  (testing "it does nothing to non-string values"
    (let [condition (o/literalize-array {:offset 1 :value [1, true, nil, 1.5] :params []})]
      (is (= {:offset 1 :params ["{1,true,null,1.5}"] :value "$1"} condition)))))