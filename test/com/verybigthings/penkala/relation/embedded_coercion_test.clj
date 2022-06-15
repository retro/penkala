(ns com.verybigthings.penkala.relation.embedded-coercion-test
  (:require [clojure.test :refer [use-fixtures deftest is testing]]
            [com.verybigthings.penkala.next-jdbc :refer [select! select-one!]]
            [com.verybigthings.penkala.relation :as r]
            [com.verybigthings.penkala.helpers :as h]
            [com.verybigthings.penkala.test-helpers :as th :refer [*env*]]
            [com.verybigthings.penkala.decomposition :refer [coerce-embedded-value]]
            [same :refer [ish? zeroish?]]))

(use-fixtures :once (partial th/reset-db-fixture "embedded_types"))

(deftest it-correctly-coerces-values-in-embedded-relations
  (let [{:keys [vals]} *env*
        s (select-one! *env* vals)
        embedded-res (select-one! *env* (-> r/empty-relation
                                            (r/extend-with-embedded :vals vals)))
        e (-> embedded-res :vals first)]
    (clojure.pprint/pprint s)
    (clojure.pprint/pprint e)
    (testing "smallint"
      (is (= (:vals/val-smallint s) (:vals/val-smallint e))))
    (testing "integer"
      (is (= (:vals/val-integer s) (:vals/val-integer e))))
    (testing "bigint"
      (is (= (:vals/val-bigint s) (:vals/val-bigint e))))
    (testing "decimal"
      (is (= (type (:vals/val-decimal s)) (type (:vals/val-decimal e))))
      (is (ish? (float (:vals/val-decimal s)) (float (:vals/val-decimal e)))))
    (testing "numeric"
      (is (= (type (:vals/val-numeric s)) (type (:vals/val-numeric e))))
      (is (ish? (float (:vals/val-numeric s)) (float (:vals/val-numeric e)))))
    (testing "numeric-precision"
      (is (= (:vals/val-numeric-precision s) (:vals/val-numeric-precision e))))
    (testing "numeric-precision-scale"
      (is (= (:vals/val-numeric-precision-scale s) (:vals/val-numeric-precision-scale e))))
    (testing "real"
      (is (= (:vals/val-real s) (:vals/val-real e))))
    (testing "double-precision"
      (is (= (:vals/val-double-precision s) (:vals/val-double-precision e))))
    (testing "smallserial"
      (is (= (:vals/val-smallserial s) (:vals/val-smallserial e))))
    (testing "serial"
      (is (= (:vals/val-serial s) (:vals/val-serial e))))
    (testing "bigserial"
      (is (= (:vals/val-bigserial s) (:vals/val-bigserial e))))
    (testing "money"
      (is (= (:vals/val-money s) (:vals/val-money e))))
    (testing "varchar"
      (is (= (:vals/val-varchar s) (:vals/val-varchar e))))
    (testing "varchar-limit"
      (is (= (:vals/val-varchar-limit s) (:vals/val-varchar-limit e))))
    (testing "char"
      (is (= (:vals/val-char s) (:vals/val-char e))))
    (testing "text"
      (is (= (:vals/val-text s) (:vals/val-text e))))

    (testing "timestamp with time zone"
      (is (= (:vals/val-timestamp-with-time-zone s)
             (:vals/val-timestamp-with-time-zone e))))
    (testing "timestamp with time zone 0 precision"
      (is (= (:vals/val-timestamp-with-time-zone-0-precision s)
             (:vals/val-timestamp-with-time-zone-0-precision e))))
    (testing "timestamp with time zone 1 precision"
      (is (= (:vals/val-timestamp-with-time-zone-1-precision s)
             (:vals/val-timestamp-with-time-zone-1-precision e))))
    (testing "timestamp with time zone 2 precision"
      (is (= (:vals/val-timestamp-with-time-zone-2-precision s)
             (:vals/val-timestamp-with-time-zone-2-precision e))))
    (testing "timestamp with time zone 3 precision"
      (is (= (:vals/val-timestamp-with-time-zone-3-precision s)
             (:vals/val-timestamp-with-time-zone-3-precision e))))
    (testing "timestamp with time zone 4 precision"
      (is (= (:vals/val-timestamp-with-time-zone-4-precision s)
             (:vals/val-timestamp-with-time-zone-4-precision e))))
    (testing "timestamp with time zone 5 precision"
      (is (= (:vals/val-timestamp-with-time-zone-5-precision s)
             (:vals/val-timestamp-with-time-zone-5-precision e))))
    (testing "timestamp with time zone 6 precision"
      (is (= (:vals/val-timestamp-with-time-zone-6-precision s)
             (:vals/val-timestamp-with-time-zone-6-precision e))))
    (testing "timestamp with time zone"
      (is (= (:vals/val-timestamp-with-time-zone s)
             (:vals/val-timestamp-with-time-zone e))))
    (testing "timestamp without time zone 0 precision"
      (is (= (:vals/val-timestamp-without-time-zone-0-precision s)
             (:vals/val-timestamp-without-time-zone-0-precision e))))
    (testing "timestamp without time zone 1 precision"
      (is (= (:vals/val-timestamp-without-time-zone-1-precision s)
             (:vals/val-timestamp-without-time-zone-1-precision e))))
    (testing "timestamp without time zone 2 precision"
      (is (= (:vals/val-timestamp-without-time-zone-2-precision s)
             (:vals/val-timestamp-without-time-zone-2-precision e))))
    (testing "timestamp without time zone 3 precision"
      (is (= (:vals/val-timestamp-without-time-zone-3-precision s)
             (:vals/val-timestamp-without-time-zone-3-precision e))))
    (testing "timestamp without time zone 4 precision"
      (is (= (:vals/val-timestamp-without-time-zone-4-precision s)
             (:vals/val-timestamp-without-time-zone-4-precision e))))
    (testing "timestamp without time zone 5 precision"
      (is (= (:vals/val-timestamp-without-time-zone-5-precision s)
             (:vals/val-timestamp-without-time-zone-5-precision e))))
    (testing "timestamp without time zone 6 precision"
      (is (= (:vals/val-timestamp-without-time-zone-6-precision s)
             (:vals/val-timestamp-without-time-zone-6-precision e))))))

(deftest coerce-embedded-money
  (is (= 99.99 (coerce-embedded-value "money" "$99.99")))
  (is (= 99.99 (coerce-embedded-value "money" "99.99€")))
  (is (= 99.99 (coerce-embedded-value "money" "$99.99€")))
  (is (= 99.99 (coerce-embedded-value "money" "99,99€"))))