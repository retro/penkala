(ns com.verybigthings.penkala.helpers
  (:require [com.verybigthings.penkala.relation :refer [->Wrapped]]
            [com.verybigthings.penkala.util :refer [q]]
            [clojure.string :as str]))

(defn column
  "Mark the subject as a column"
  [subject]
  (->Wrapped :column subject))

(defn value
  "Mark the subject as a value"
  [subject]
  (->Wrapped :value subject))

(defn param
  "Mark the subject as a param"
  [subject]
  (->Wrapped :param subject))

(defn literal
  "Mark the subject as a literal"
  [subject]
  (->Wrapped :literal subject))

(defn unary-operator
  "Mark the subject as an unary operator"
  [subject]
  (->Wrapped :unary-operator subject))

(defn binary-operator
  "Mark the subject as a binary operator"
  [subject]
  (->Wrapped :binary-operator subject))

(defn ternary-operator
  "Mark the subject as a ternary operator"
  [subject]
  (->Wrapped :ternary-operator subject))

(defn quoted-literal
  "Quote the subject and mark it as a literal"
  [subject]
  (let [subject' (if (keyword? subject) (name subject) subject)]
    (->Wrapped :literal (str "'" subject' "'"))))

(def match-to-escape-re (re-pattern "[,\\{}\\s\\\\\"]"))
(def escape-re (re-pattern "([\\\\\"])"))

(defn array-literal
  "Stringify a collection as PostgreSQL array and mark it as a literal"
  [value]
  (let [sanitized-values
        (map
          (fn [v]
            (cond
              (nil? v)
              "null"

              (or (= "" v) (= "null" v) (and (string? v) (re-find match-to-escape-re v)))
              (-> v (str/replace escape-re "$1") q)

              :else v))
          value)]
    (quoted-literal (str "{" (str/join "," sanitized-values) "}"))))

(def l literal)
(def ql quoted-literal)