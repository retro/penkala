(ns com.verybigthings.penkala.helpers
  (:require [com.verybigthings.penkala.relation :refer [->Wrapped]]
            [com.verybigthings.penkala.util :refer [q]]
            [clojure.string :as str]))

(defn column [subject]
  (->Wrapped :column subject))

(defn value [subject]
  (->Wrapped :value subject))

(defn param [subject]
  (->Wrapped :param subject))

(defn literal [subject]
  (->Wrapped :literal subject))

(defn unary-operator [subject]
  (->Wrapped :unary-operator subject))

(defn binary-operator [subject]
  (->Wrapped :binary-operator subject))

(defn ternary-operator [subject]
  (->Wrapped :ternary-operator subject))

(defn quoted-literal [subject]
  (let [subject' (if (keyword? subject) (name subject) subject)]
    (->Wrapped :literal (str "'" subject' "'"))))

(def match-to-escape-re (re-pattern "[,\\{}\\s\\\\\"]"))
(def escape-re (re-pattern "([\\\\\"])"))

(defn array-literal [value]
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