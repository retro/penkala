(ns com.verybigthings.penkala.statement.operations
  (:require [clojure.string :as str]
            [com.verybigthings.penkala.util.core :refer [str-quote]]))

(defn cast-timestamp [value]
  (if (inst? value) "::timestamptz" ""))

(defn build-in [condition]
  (let [operator (get-in condition [:appended :operator])
        {:keys [value]} condition]
    (if-not (seq value)
      (assoc condition :value (if (= "=" operator) "ANY ('{}')" "ALL ('{}')"))
      (let [in-list (reduce-kv
                      (fn [acc idx v]
                        (conj acc (str "?" (cast-timestamp v))))
                      []
                      (->> value (map-indexed (fn [i v] [i v])) (into {})))]
        (-> condition
          (assoc-in [:appended :operator] (if (= "=" operator) "IN" "NOT IN"))
          (assoc :value (str "(" (str/join "," in-list) ")")
                 :params value))))))

(defn build-is [condition]
  (update-in condition [:appended :operator] #(if (contains? #{"=" "IS"} %) "IS" "IS NOT")))

(defn equality [condition]
  (let [{:keys [value]} condition]
    (cond
      (or (nil? value) (boolean? value))
      (build-is condition)

      (sequential? value)
      (build-in condition)

      :else
      (-> condition
        (update :params conj value)
        (assoc :value (str "?" (cast-timestamp value)))))))

(defn build-between [{:keys [value]}]
  (let [[f s] value]
    {:params value
     :value (str "?" (cast-timestamp f) " AND ?" (cast-timestamp s))}))

(def match-to-escape-re (re-pattern "[,\\{}\\s\\\\\"]"))
(def escape-re (re-pattern "([\\\\\"])"))

(defn literalize-array [condition]
  (let [{:keys [value]} condition]
    (if (sequential? value)
      (let [sanitized-values
            (map
              (fn [v]
                (cond
                  (nil? v)
                  "null"

                  (or (= "" v) (= "null" v) (and (string? v) (re-find match-to-escape-re v)))
                  (-> v (str/replace escape-re "$1") str-quote)

                  :else v))
              value)]
        (-> condition
          (update :params conj (str "{" (str/join "," sanitized-values) "}"))
          (assoc :value (str "?" (cast-timestamp value)))))
      (-> condition
        (update :params conj value)
        (assoc :value (str "?" (cast-timestamp value)))))))

(defn default-param-mutator [{:keys [value] :as condition}]
  (-> condition
    (update :params conj value)
    (assoc :value (str "?" (cast-timestamp value)))))

(def operations
  {"=" {:operator "=" :mutator equality}
   "!" {:operator "<>" :mutator equality}
   ">" {:operator ">" :mutator default-param-mutator}
   "<" {:operator "<" :mutator default-param-mutator}
   ">=" {:operator ">=" :mutator default-param-mutator}
   "<=" {:operator "<=" :mutator default-param-mutator}
   "!=" {:operator "<>" :mutator equality}
   "<>" {:operator "<>" :mutator equality}
   "between" {:operator "BETWEEN" :mutator build-between}
   "@>" {:operator "@>" :mutator literalize-array}
   "<@" {:operator "<@" :mutator literalize-array}
   "&&" {:operator "&&" :mutator literalize-array}
   "~~" {:operator "LIKE" :mutator default-param-mutator}
   "like" {:operator "LIKE" :mutator default-param-mutator}
   "!~~" {:operator "NOT LIKE" :mutator default-param-mutator}
   "not like" {:operator "NOT LIKE" :mutator default-param-mutator}
   "~~*" {:operator "ILIKE" :mutator default-param-mutator}
   "ilike" {:operator "ILIKE" :mutator default-param-mutator}
   "!~~*" {:operator "NOT ILIKE" :mutator default-param-mutator}
   "not ilike" {:operator "NOT ILIKE" :mutator default-param-mutator}
   "similar to" {:operator "SIMILAR TO" :mutator default-param-mutator}
   "not similar to" {:operator "NOT SIMILAR TO" :mutator default-param-mutator}
   "~" {:operator "~" :mutator default-param-mutator}
   "!~" {:operator "!~" :mutator default-param-mutator}
   "~*" {:operator "~*" :mutator default-param-mutator}
   "!~*" {:operator "!~*" :mutator default-param-mutator}
   "is" {:operator "IS" :mutator build-is}
   "is not" {:operator "IS NOT" :mutator build-is}
   "is distinct from" {:operator "IS DISTINCT FROM"}
   "is not distinct from" {:operator "IS NOT DISTINCT FROM"}})