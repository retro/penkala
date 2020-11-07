(ns com.verybigthings.penkala.util.core
  (:require [clojure.string :as str]))

(def join-separator "__")

(defn path-prefix-join [path]
  (str/join join-separator path))

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
  (if (sequential? val) (vec val) [val]))

(defn expand-join-path [path]
  (mapcat (fn [v] [:state :joins (keyword v) :relation]) path))