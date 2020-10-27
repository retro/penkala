(ns com.verybigthings.penkala.relation
  (:require [com.verybigthings.penkala.util.core :refer [str-quote as-vec]]
            [clojure.spec.alpha :as s]
            [com.verybigthings.penkala.util.parse-key :as p]
            [com.verybigthings.penkala.statement.operations :as o]
            [clojure.string :as str]))

(s/def ::criteria-map
  (s/map-of #(or (string? %) (keyword? %)) any?))

(s/def ::or-where
  (s/and
    vector?
    (s/cat
      :or #(= :or %)
      :elements (s/+ ::where))))

(s/def ::and-where
  (s/and
    vector?
    (s/cat
      :and #(= :and %)
      :elements (s/+ ::where))))

(s/def ::where
  (s/or
    :criteria-map ::criteria-map
    :or-where ::or-where
    :and-where ::and-where))

;; Keys in spec will come without namespaces
(defn make-relation [db spec]
  (let [current-schema (:db/schema db)
        entity-schema  (:schema spec)
        entity-name    (:name spec)
        path           (or (:path spec)
                         (if (= current-schema entity-schema) entity-name (str entity-schema "." entity-name)))
        schema         (or entity-schema current-schema)
        delimited-name (str-quote entity-name)
        delimited-schema (str-quote schema)
        delimited-full-name (if (= schema current-schema) delimited-name (str delimited-schema "." delimited-name))
        column-names (set (:columns spec))
        columns (mapv (fn [c] {:column/schema schema
                               :column/parent entity-name
                               :column/name c
                               :column/full-name (str delimited-full-name "." c)})
                  column-names)]
    {:db/schema schema
     :db.schema/current current-schema
     :relation/name entity-name
     :relation/loader (:loader spec)
     :relation/delimited-name delimited-name
     :relation/delimited-schema delimited-schema
     :relation/delimited-full-name delimited-full-name
     :relation/path path
     :relation/is-mat-view (boolean (:is_matview spec))
     :relation/column-names column-names
     :relation/columns columns
     :relation/pk (when-let [pk (:pk spec)] (as-vec pk))
     :relation/fks (set (:fks spec))}))

(defn get-column-name [db-config relation column-name]
  (let [db-schema (:db/schema db-config)
        relation-schema (:db/schema relation)
        relation-name (:relation/name relation)
        relation-alias (:relation/alias relation)]
    (cond
      relation-alias (str (str-quote relation-alias) "." (str-quote column-name))
      (= db-schema relation-schema) (str (str-quote relation-name) "." (str-quote column-name))
      :else (str (str-quote relation-schema) "." (str-quote relation-name) "." (str-quote column-name)))))

(defn get-column-alias [relation column-name]
  (let [relation-alias (:relation/alias relation)]
    (if relation-alias
      (str-quote (str relation-alias "__" column-name))
      (str-quote column-name))))

(defn compile-predicate
  ([relation predicate] (compile-predicate relation predicate nil))
  ([relation predicate join-key]
   (cond
     (map? predicate)
     (let [p-acc (reduce-kv
                   (fn [p-acc k v]
                     (let [lhs-relation (if join-key (get-in relation [:relation/joins join-key]) relation)
                           parsed-v (when (or (string? v) (keyword? v)) (p/parse-key relation (name v)))
                           parsed-k (p/with-appendix lhs-relation (name k) o/operations v)]
                       (if (contains? (:relation/column-names relation) (:query/field parsed-v))
                         (conj p-acc (assoc parsed-k :value/field parsed-v))
                         (conj p-acc parsed-k))))
                   []
                   predicate)]
       (cond
         (= 1 (count p-acc)) (first p-acc)
         (seq p-acc) (into [:and] p-acc)
         :else nil))

     (vector? predicate)
     (let [[op & predicates] predicate
           p-acc (reduce
                   (fn [p-acc v]
                     (let [compiled (compile-predicate relation v)]
                       (if compiled
                         (conj p-acc compiled)
                         p-acc)))
                   []
                   predicates)]
       (cond
         (= 1 (count p-acc)) (first p-acc)
         (seq p-acc) (into [op] p-acc)
         :else nil))
     :else nil)))

;; TODO literal wrapper
(defn literal [val])

(defn where [relation restriction]
  (s/assert ::where restriction)
  (assoc relation :query/where (compile-predicate relation restriction)))

#_(defn and-where [relation restriction]
  (s/assert ::where restriction)
  (assoc relation :query/where [:and (:query/where relation)]))

#_(defn or-where [relation restriction]
  (s/assert ::where restriction)
  (assoc relation :query/where [:or (:query/where relation)]))

(defn limit [relation limit]
  (assoc relation :query/limit limit))

(defn inner-join [relation join-key join-relation join-restriction]
  (s/assert ::where join-restriction)
  (let [join-relation' (assoc join-relation :relation/alias (name join-key) :join/kind :inner)
        relation' (assoc-in relation [:relation/joins join-key] join-relation')]
    (assoc-in relation' [:relation/joins join-key :join/on] (compile-predicate relation' join-restriction join-key))))