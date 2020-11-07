(ns com.verybigthings.penkala.rel
  (:require [clojure.string :as str]))

(def ^:dynamic *relation-path* [])
(def ^:dynamic *relation* nil)

(defn unnamed-relation-alias [relation]
  (str "unnamed-relation-" (System/identityHashCode relation)))

(defprotocol IRelation
  (base? [this])
  (get-spec [this])
  (get-relation-name [this])
  (get-schema [this])
  (get-where [this])
  (get-projection [this])
  (get-joins [this]))

(defn get-path [& parts]
  (str/join "__" (concat (map get-relation-name *relation-path*) parts)))

(defrecord Column [relation column])
(defrecord Aggregate [function column rename])
(defrecord Join [join-type relation predicate])
(defrecord Projection [columns])

{:schema "public",
 :is_insertable_into true,
 :fk_origin_columns nil,
 :pk ["id"],
 :parent nil,
 :columns ["id" "notes" "ordered_at" "product_id" "user_id"],
 :name "orders",
 :fk_dependent_columns nil,
 :fk_origin_schema nil,
 :fk_origin_name nil,
 :fk nil}

(deftype Relation [spec where projection joins]
  IRelation
  (base? [_] (and (nil? where) (nil? joins)))
  (get-spec [_] spec)
  (get-schema [_] (:schema spec))
  (get-relation-name [_] (:name spec))
  (get-where [_] where)
  (get-projection [_] projection)
  (get-joins [_] joins)
  clojure.lang.ILookup
  (valAt [this k]
    (->Column this (name k))))

(defmethod print-method Relation [v ^java.io.Writer w]
  (let [printed (clojure.pprint/pprint {:spec (get-spec v) :where (get-where v) :projection (get-projection v) :joins (get-joins v)})]
    (print-method printed w)))

(defn underscore [s]
  (str/replace s #"-" "_"))

(defn spec->relation [relation-name spec]
  (->Relation (assoc spec :alias relation-name) nil (->Projection (:columns spec)) nil))

#_(defmacro defrelation [relation-name relation-name columns]
    `(def ~relation-name
       (Relation. {:relation/relation-name ~relation-name
                   :relation.relation-name/alias (if (string? ~relation-name) ~relation-name (unnamed-relation-alias ~relation-name))
                   :relation/alias (name '~relation-name)}
         nil
         (->Projection (:relation/columns spec)) nil)))

;;;;;; convenience methods

(defn project [relation columns]
  (let [spec       (get-spec relation)
        where      (get-where relation)
        projection (->Projection columns)
        joins      (get-joins relation)]
    (Relation. spec where projection joins)))

(defn where [relation predicate]
  (let [spec       (get-spec relation)
        where      predicate
        projection (get-projection relation)
        joins      (get-joins relation)]
    (Relation. spec where projection joins)))

(defn join [relation join-type alias joined-relation predicate]
  (let [spec       (get-spec relation)
        where      (get-where relation)
        projection (get-projection relation)
        joins      (assoc (get-joins relation) alias (->Join join-type joined-relation predicate))]
    (Relation. spec where projection joins)))

(defn inner-join [relation alias joined-relation predicate]
  (join relation :inner alias joined-relation predicate))

(defn left-join [relation alias joined-relation predicate]
  (join relation :left alias joined-relation predicate))

(defn maximum [column rename]
  (->Aggregate :max column rename))

;;;;;; SQL

(declare to-sql)

(defn wrap-parens [s]
  (str "(" s ")"))

(defn wrap-quotes [s]
  (str "\"" s "\""))

(defn group-by-sql [p]
  (let [cols         (:columns p)
        aggs         (filter #(= (type %) Aggregate) cols)
        non-aggs     (filter #(= (type %) Column) cols)
        non-aggs-sql (map #(to-sql %) non-aggs)
        sql          (when (and (not-empty aggs) (not-empty non-aggs)) (str " GROUP BY " (str/join ", " non-aggs-sql)))]
    sql))

(defmulti to-sql (fn [v & _] (type v)))

(defmethod to-sql nil [_])

(defmethod to-sql clojure.lang.Keyword [k]
  (name k))

(defmethod to-sql java.lang.String [s]
  (str "\"" s "\""))

(defmethod to-sql java.lang.Long [n]
  n)

(defmethod to-sql Column [c]
  (let [column (:column c)]
    (str (get-path) "." column)))

(defmethod to-sql Aggregate [a]
  (let [function (name (:function a))
        column   (to-sql (:column a))
        rename   (to-sql (:rename a))]
    (str function (wrap-parens column) " AS " rename)))

(defmethod to-sql Projection [p relation]
  (let [raw-columns (:columns p)
        relation-name (get-relation-name relation)
        sql-columns (map #(str (to-sql relation-name) "." (to-sql %) " AS " (get-path %)) raw-columns)
        join-sql-columns (map (fn [j]
                                (let [r (:relation j)
                                      p (get-projection r)]
                                  (binding [*relation-path* (conj *relation-path* r)]
                                    (to-sql p r))))
                           (vals (get-joins relation)))]
    (str/join ", " (concat sql-columns join-sql-columns))))

(defmethod to-sql clojure.lang.PersistentVector [p]
  (let [operator      (first p)
        operands      (rest p)
        predicate-sql (if (contains? #{:and :or} operator)
                        (wrap-parens (str/join (str " " (str/upper-case (name operator)) " ") (map #(to-sql %) operands)))
                        (wrap-parens (str/join " " [(to-sql (first operands))
                                                    (name operator)
                                                    (to-sql (last operands))])))]
    predicate-sql))

(defmethod to-sql Join [j]
  (let [relation      (:relation j)
        predicate     (:predicate j)
        sql-join-type ({:left "LEFT OUTER" :inner "INNER"} (:join-type j))]
    (->> [sql-join-type
          "JOIN"
          (if (base? relation)
            (get-relation-name relation)
            (binding [*relation-path* []] (to-sql relation)))
          (get-path (get-relation-name relation))
          "ON"
          (to-sql predicate)]
      (str/join " ")
      (str " "))))

(defmethod to-sql Relation [relation]
  (let [relation-name   (get-relation-name relation)
        projection      (get-projection relation)
        where           (get-where relation)
        joins           (get-joins relation)
        select-clause   (str "SELECT " (to-sql projection relation))
        from-clause     (str " FROM " (to-sql relation-name) " " (get-path))
        join-clause     (str/join " " (map to-sql (vals joins)))
        where-clause    (when-not (empty? where) (str " WHERE " (to-sql where)))
        group-by-clause (group-by-sql projection)]
    (wrap-parens (str select-clause
                   from-clause
                   join-clause
                   where-clause
                   group-by-clause))))
