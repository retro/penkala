(ns com.verybigthings.penkala.statement.select
  (:require [clojure.string :as str]
            [com.verybigthings.penkala.util.parse-key :as p]
            [com.verybigthings.penkala.relation :as r]
            [com.verybigthings.penkala.util.decompose :as d]
            [com.verybigthings.penkala.util.core :refer [str-quote]]))

(defn get-relation-projection [db relation]
  (mapv
    (fn [c]
      (let [column-name (r/get-column-name db relation c)
            column-as (r/get-column-alias relation c)]
        (str column-name " AS " column-as)))
    (:relation/column-names relation)))

(defn get-projection [db relation]
  (reduce-kv
    (fn [projection _ join-relation]
      (let [with-join-projection (into projection (get-relation-projection db join-relation))]
        (if (:relation/joins join-relation)
          (into with-join-projection (get-projection db join-relation))
          with-join-projection)))
    (get-relation-projection db relation)
    (:relation/joins relation)))

(defn add-projection [acc db relation]
  (let [projection (get-projection db relation)]
    (update acc :sql conj (str (str/join ", " projection)))))

(defn add-from [acc relation]
  (update acc :sql conj (str "FROM " (:relation/delimited-full-name relation))))

(defn get-predicate [acc db relation predicate-part]
  (cond
    (and (map? predicate-part) (:value/field predicate-part))
    (let [value-field (:value/field predicate-part)
          operator (get-in predicate-part [:appended :operator])]
      (update acc :sql conj (str (p/get-lhs predicate-part relation db) " " operator " " (p/get-lhs value-field relation db))))

    (map? predicate-part)
    (let [mutator (get-in predicate-part [:appended :mutator])
          parsed  (mutator predicate-part)]
      (-> acc
        (update :sql conj (str (p/get-lhs parsed relation db) " " (get-in parsed [:appended :operator]) " " (:value parsed)))
        (update :params into (:params parsed))))

    (vector? predicate-part)
    (let [[op & predicates] predicate-part
          sql-op ({:or "OR" :and "AND"} op)
          {:keys [sql params]} (reduce
                                 (fn [acc' predicate-part']
                                   (get-predicate acc' db relation predicate-part'))
                                 {:sql [] :params []}
                                 predicates)]
      (-> acc
        (update :sql conj (str "(" (str/join (str " " sql-op " ") sql) ")"))
        (update :params into params)))

    :else acc))

(defn add-where [acc db relation]
  (let [where (:query/where relation)
        {:keys [sql params]} (get-predicate {:sql [] :params []} db relation where)
        predicate (if (seq sql) (str/join " " sql) "TRUE")]
    (-> acc
      (update :sql conj (str "WHERE " predicate))
      (update :params into params))))

(defn add-joins [acc db relation]
  (reduce-kv
    (fn [acc' _ j]
      (let [join-kind (:join/kind j)
            join-kind-sql ({:inner "INNER JOIN" :left "LEFT JOIN"} join-kind)
            join-on (:join/on j)
            relation-alias (:relation/alias j)
            {:keys [sql params]} (get-predicate {:sql [] :params []} db relation join-on)
            predicate (if (seq sql) (str/join " " sql) "TRUE")
            acc-with-join (-> acc'
                            (update :sql conj (str join-kind-sql " " (:relation/delimited-full-name j) " " relation-alias " ON " predicate))
                            (update :params into params))]
        (if (:relation/joins j)
          (add-joins acc-with-join db j)
          acc-with-join)))
    acc
    (:relation/joins relation)))

(defn add-limit [acc relation]
  (if-let [limit (:query/limit relation)]
    (update acc :sql conj (str "LIMIT " limit))
    acc))

(defn get-query [db relation]
  (let [query
        (-> {:sql ["SELECT"] :params []}
          (add-projection db relation)
          (add-from relation)
          (add-joins db relation)
          (add-where db relation)
          (add-limit relation))]
    (into [(str/join " " (:sql query))] (:params query))))

