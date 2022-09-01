(ns com.verybigthings.penkala.util
  (:require [clojure.string :as str]
            [com.verybigthings.penkala.env :as env]
            [camel-snake-kebab.core
             :refer [->SCREAMING_SNAKE_CASE_STRING
                     ->kebab-case-string]]))

(def joins
  {:left "LEFT OUTER JOIN"
   :left-lateral "LEFT JOIN LATERAL"
   :right "RIGHT OUTER JOIN"
   :inner "INNER JOIN"
   :inner-lateral "INNER JOIN LATERAL"
   :full "FULL OUTER JOIN"
   :cross "CROSS JOIN"})

(def op->sql-op
  {"@?" "@??"
   "?" "??"
   "?|" "??|"
   "?#" "??#"
   "?-" "??-"
   "?-|" "??-|"
   "?||" "??|"})

(defn get-sql-op [op]
  (if (string? op)
    (or (op->sql-op op) op)
    (or (op->sql-op op) (-> op ->SCREAMING_SNAKE_CASE_STRING (str/replace #"_" " ")))))

(defn str-quote [s]
  (str "\"" (if (keyword? s) (name s) s) "\""))

(def q str-quote)

(defn make-rel-alias-prefix [_]
  (str (gensym "sq_")))

(defn get-rel-alias-with-prefix [env rel-alias]
  (if-let [rel-alias-prefix (-> env ::relation-alias-prefix last)]
    (str rel-alias-prefix "__" rel-alias)
    rel-alias))

(defn get-rel-alias [rel]
  (get-in rel [:spec :name]))

(defn get-rel-schema [env rel]
  (let [current-schema (::env/schema env)
        schema-renames (::env/schema-renames env)
        original-schema (get-in rel [:spec :schema])
        renamed-schema (if-let [r (get schema-renames original-schema)] r original-schema)]
    (when (not= current-schema renamed-schema)
      renamed-schema)))

(defn get-schema-qualified-relation-name [env rel]
  (let [rel-name       (get-in rel [:spec :name])]
    (if-let [rel-schema (get-rel-schema env rel)]
      (str (q rel-schema) "." (q rel-name))
      (q rel-name))))

(defn get-relation-name [rel]
  (q (get-in rel [:spec :name])))

(def join-separator "__")
(def join-separator-re #"__")

(defn path-prefix-join [path]
  (str/join join-separator path))

(defn path-prefix-split [str]
  (str/split str join-separator-re))

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

(defn join-space [val]
  (str/join " " val))

(defn join-comma [val]
  (str/join ", " val))

(defn wrap-parens [& args]
  (str "(" (apply str args) ")"))

(def spc " ")