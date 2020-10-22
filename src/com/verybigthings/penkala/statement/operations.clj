(ns com.verybigthings.penkala.statement.operations
  (:require [clojure.string :as str]))

(defn cast-timestamp [value]
  (if (inst? value) "::timestamptz" ""))

(defn build-in [condition]
  (let [operator (get-in condition [:appended :operator])
        {:keys [value offset]} condition]
    (if-not (seq value)
      (assoc condition :value (if (= "=" operator) "ANY ('{}')" "ALL ('{}')"))
      (let [in-list (reduce-kv
                      (fn [acc idx v]
                        (conj acc (str "$" (+ idx offset) (cast-timestamp v))))
                      []
                      (->> value (map-indexed (fn [i v] [i v])) (into {})))]
        (-> condition
          (assoc-in [:appended :operator] (if (= "=" operator) "IN" "NOT IN"))
          (assoc :value (str "(" (str/join "," in-list) ")")
                 :params value
                 :offset (+ offset (count value))))))))

(defn build-is [condition]
  (update-in condition [:appended :operator] #(if (contains? #{"=" "IS"} %) "IS" "IS NOT")))

(defn equality [condition]
  (let [{:keys [value offset]} condition]
    (cond
      (or (nil? value) (boolean? value))
      (build-is condition)

      (sequential? value)
      (build-in condition)

      :else
      (-> condition
        (update :params conj value)
        (assoc :value (str "$" offset (cast-timestamp value)))))))

(defn build-between [{:keys [value offset]}]
  (let [[f s] value]
    {:params value
     :value (str "$" offset (cast-timestamp f) " AND $" (inc offset) (cast-timestamp s))
     :offset (+ 2 offset)}))

(defn literalize-array [condition])


(def operations
  {"=" {:operator "=" :mutator equality}
   "!" {:operator "<>" :mutator equality}
   ">" {:operator ">"}
   "<" {:operator "<"}
   ">=" {:operator ">="}
   "<=" {:operator "<="}
   "!=" {:operator "<>" :mutator equality}
   "<>" {:operator "<>" :mutator equality}
   "between" {:operator "BETWEEN" :mutator build-between}
   "@>" {:operator "@>" :mutator literalize-array}
   "<@" {:operator "<@" :mutator literalize-array}
   "&&" {:operator "&&" :mutator literalize-array}
   "~~" {:operator "LIKE"}
   "like" {:operator "LIKE"}
   "!~~" {:operator "NOT LIKE"}
   "not like" {:operator "NOT LIKE"}
   "~~*" {:operator "ILIKE"}
   "ilike" {:operator "ILIKE"}
   "!~~*" {:operator "NOT ILIKE"}
   "not ilike" {:operator "NOT ILIKE"}
   "similar to" {:operator "SIMILAR TO"}
   "not similar to" {:operator "NOT SIMILAR TO"}
   "~" {:operator "~"}
   "!~" {:operator "!~"}
   "~*" {:operator "~*"}
   "!~*" {:operator "!~*"}
   "is" {:operator "IS" :mutator build-is}
   "is not" {:operator "IS NOT" :mutator build-is}
   "is distinct from" {:operator "IS DISTINCT FROM"}
   "is not distinct from" {:operator "IS NOT DISTINCT FROM"}})