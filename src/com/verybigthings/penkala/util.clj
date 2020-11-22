(ns com.verybigthings.penkala.util
  (:require [clojure.string :as str]
            [camel-snake-kebab.core :refer [->kebab-case-string]]))

(def joins
  {:left "LEFT OUTER JOIN"
   :left-lateral "LEFT JOIN LATERAL"
   :right "RIGHT OUTER JOIN"
   :inner "INNER JOIN"
   :inner-lateral "INNER JOIN LATERAL"
   :full "FULL OUTER JOIN"
   :cross "CROSS JOIN"})

(def join-separator "__")
(def join-separator-re #"__")

(defn path-prefix-join [path]
  (str/join join-separator path))

(defn path-prefix-split [str]
  (str/split str join-separator-re))

(defn str-quote [s]
  (str "\"" (if (keyword? s) (name s) s) "\""))

(def q str-quote)

(defn select-keys-with-default [m ks default]
  (reduce
    (fn [acc k]
      (assoc acc k (get m k default)))
    {}
    ks))

(defn as-vec [val]
  (when val
    (if (sequential? val) (vec val) [val])))

(defn expand-join-path [path]
  (mapcat (fn [v] [:joins (keyword v) :relation]) path))

(def vec-conj (fnil conj []))

(defn col->alias [col]
  (->> col
    name
    path-prefix-split
    (map ->kebab-case-string)
    path-prefix-join
    keyword))