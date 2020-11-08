(ns com.verybigthings.penkala.rel2
  (:refer-clojure :exclude [group-by extend distinct])
  (:require [clojure.spec.alpha :as s]
            [camel-snake-kebab.core :refer [->kebab-case-keyword ->kebab-case-string]]
            [clojure.string :as str]
            [clojure.walk :refer [prewalk]]
            [com.verybigthings.penkala.util.core :refer [expand-join-path]]))

(defn ex-info-missing-column [rel node]
  (let [column-name (if (keyword? node) node (:subject node))]
    (ex-info (str "Column " column-name " doesn't exist") {:column column-name :rel rel})))

(defrecord Wrapped [subject-type subject])

(defn column [subject]
  (->Wrapped :column subject))

(defn value [subject]
  (->Wrapped :value subject))

(defn param [subject]
  (->Wrapped :param subject))

(def operations
  {:unary #{:is-null :is-not-null :is-true :is-not-true :is-false :is-not-false :is-unknown :is-not-unknown}
   :binary #{:< :> :<= :>= := :<> :!=}
   :ternary #{:between :not-between :between-symmetric :not-between-symmetric :is-distinct-from :is-not-distinct-from}})

(s/def ::connective
  (s/and
    vector?
    (s/cat
      :op #(contains? #{:and :or} %)
      :args (s/+ ::value-expression))))

(s/def ::negation
  (s/and
    vector?
    (s/cat
      :op #(= :not %)
      :arg1 ::value-expression)))

(s/def ::unary-operation
  (s/and
    vector?
    (s/cat
      :op #(contains? (:unary operations) %)
      :arg1 ::value-expression)))

(s/def ::binary-operation
  (s/and
    vector?
    (s/cat
      :op #(contains? (:binary operations) %)
      :arg1 ::value-expression
      :arg2 ::value-expression)))

(s/def ::ternary-operation
  (s/and
    vector?
    (s/cat
      :op #(contains? (:ternary operations) %)
      :arg1 ::value-expression
      :arg2 ::value-expression
      :arg3 ::value-expression)))

(s/def ::function-call
  (s/and
    vector?
    (s/cat
      :fn keyword?
      :args (s/+ ::value-expression))))

(s/def ::wrapped-column
  #(and (= Wrapped (type %)) (= :column (:subject-type %))))

(s/def ::wrapped-value
  #(and (= Wrapped (type %)) (= :value (:subject-type %))))

(s/def ::wrapped-param
  #(and (= Wrapped (type %)) (= :param (:subject-type %))))

(s/def ::column-identifier
  (s/or
    :keyword keyword?
    :wrapped-column ::wrapped-column))

(s/def ::order-direction
  #(contains? #{:asc :desc} %))

(s/def ::order-nulls
  #(contains? #{:nulls-first :nulls-last} %))

(s/def ::order
  (s/or
    :column-identifier ::column-identifier
    :column (s/cat
              :column-identifier ::column-identifier)
    :column-direction (s/cat
                        :column-identifier ::column-identifier
                        :order-direction ::order-direction)
    :column-direction-nulls (s/cat
                              :column-identifier ::column-identifier
                              :order-direction ::order-direction
                              :order-nulls ::order-nulls)))

(s/def ::orders
  (s/coll-of ::order))

(s/def ::value-expression
  (s/or
    :boolean boolean?
    :keyword keyword?
    :connective ::connective
    :negation ::negation
    :unary-operation ::unary-operation
    :binary-operation ::binary-operation
    :ternary-operation ::ternary-operation
    :function-call ::function-call
    :wrapped-column ::wrapped-column
    :wrapped-param ::wrapped-param
    :wrapped-value ::wrapped-value
    :value any?))

(s/def ::column-list
  (s/coll-of ::column-identifier))

(defn resolve-column [rel node]
  (let [column           (if (keyword? node) node (:subject node))
        column-ns        (namespace column)
        column-join-path (when (seq column-ns) (str/split column-ns #"\."))
        column-name (name column)]
    (if (seq column-join-path)
      (let [column-join-path' (map keyword column-join-path)
            column-rel (get-in rel (expand-join-path column-join-path'))
            id (get-in column-rel [:column-aliases (keyword column-name)])]
        (when id
          {:path column-join-path' :id id :name column-name :original column}))
      (when-let [id (get-in rel [:column-aliases (keyword column-name)])]
        {:id id :name column-name :original column}))))

(defn process-value-expression [rel vex]
  (prewalk
    (fn [node]
      (if (vector? node)
        (let [[node-type & args] node]
          (cond
            (= :wrapped-column node-type)
            (let [column (resolve-column rel (first args))]
              (when (nil? column)
                (throw (ex-info-missing-column rel node)))
              [:resolved-column column])

            (= :wrapped-value node-type)
            [:value (-> args first :subject)]

            (= :wrapped-param node-type)
            [:param (-> args first :subject)]

            (= :binary-operation node-type)
            (if (= :!= (-> args first :op))
              (assoc-in node [1 :op] :<>)
              node)

            (= :keyword node-type)
            (let [column (resolve-column rel (first args))]
              (if column
                [:resolved-column column]
                node))
            :else node))
        node))
    vex))

(defn process-projection [rel node-list]
  (reduce
    (fn [acc [_ node]]
      (let [column (resolve-column rel node)]
        (if (or (nil? column) (-> column :path seq))
          (throw (ex-info-missing-column rel node))
          (conj acc (if (keyword? node) node (:subject node))))))
    #{}
    node-list))

(defn process-orders [rel orders]
  (map
    (fn [[node-type node]]
      (let [node' (if (= :column-identifier node-type) {:column-identifier node} node)
            column-identifier (-> node' :column-identifier second)
            column (resolve-column rel column-identifier)]
        (when (nil? column)
          (throw (ex-info-missing-column rel column-identifier)))
        (assoc node' :column [:resolved-column column])))
    orders))

(defn resolve-columns [rel columns]
  (reduce
    (fn [acc [_ node]]
      (let [column (resolve-column rel node)]
        (if (or (nil? column))
          (throw (ex-info-missing-column rel node))
          (conj acc [:resolved-column column]))))
    []
    columns))

(defprotocol IRelation
  (join [this join-type join-rel join-alias] [this join-type join-rel join-alias join-on])
  (where [this where-expression])
  (or-where [this where-expression])
  ;;(having [this vex])
  ;;(or-having [this vex])
  (offset [this offset])
  (limit [this limit])
  (order-by [this orders])
  (extend [this col-name extend-expression])
  (extend-with-aggregate [this col-name agg-fn agg-expression])
  (rename [this prev-col-name next-col-name])
  (select [this projection])
  (distinct [this] [this distinct-expression])
  (only [this] [this is-only]))

(defrecord Relation [spec]
  IRelation
  (join [this join-type join-rel join-alias]
    (throw (ex-info "Join without clause not implemented" {})))
  (join [this join-type join-rel join-alias join-on]
    (s/assert ::value-expression join-on)
    (let [with-join (assoc-in this [:joins join-alias] {:relation join-rel :type join-type})
          join-on' (process-value-expression with-join (s/conform ::value-expression join-on))]
      (assoc-in with-join [:joins join-alias :on] join-on')))
  (where [this where-expression]
    (s/assert ::value-expression where-expression)
    (let [prev-where      (:where this)
          processed-where (process-value-expression this (s/conform ::value-expression where-expression))]
      (if prev-where
        (assoc this :where [:connective {:op :and :args [prev-where processed-where]}])
        (assoc this :where processed-where))))
  (or-where [this where-expression]
    (if-let [prev-where (:where this)]
      (let [processed-where (process-value-expression this (s/conform ::value-expression where-expression))]
        (assoc this :where [:connective {:op :or :args [prev-where processed-where]}]))
      (where this where-expression)))
  (rename [this prev-col-name next-col-name]
    (let [id (get-in this [:column-aliases prev-col-name])]
      (when (nil? id)
        (throw (ex-info-missing-column this prev-col-name)))
      (cond-> (update this :column-aliases #(-> % (dissoc prev-col-name) (assoc next-col-name id)))
        (contains? (:projection this) prev-col-name)
        (update :projection #(-> % (disj prev-col-name) (conj next-col-name))))))
  (extend [this col-name extend-expression]
    (s/assert ::value-expression extend-expression)
    (when (contains? (:column-aliases this) col-name)
      (throw (ex-info (str "Column " col-name " already-exists") {:column col-name :relation this})))
    (let [processed-extend (process-value-expression this (s/conform ::value-expression extend-expression))
          id (keyword (gensym "column-"))]
      (-> this
        (assoc-in [:columns id] {:type :virtual :value-expression processed-extend})
        (assoc-in [:column-aliases col-name] id)
        (update :projection conj col-name))))
  (extend-with-aggregate [this col-name agg-fn agg-expression]
    (s/assert ::value-expression agg-expression)
    (when (contains? (:column-aliases this) col-name)
      (throw (ex-info (str "Column " col-name " already-exists") {:column col-name :relation this})))
    (let [processed-agg (process-value-expression this (s/conform ::value-expression agg-expression))
          id (keyword (gensym "column-"))]
      (-> this
        (assoc-in [:columns id] {:type :aggregate :value-expression [:function-call {:fn agg-fn :args [processed-agg]}]})
        (assoc-in [:column-aliases col-name] id)
        (update :projection conj col-name))))
  (select [this projection]
    (s/assert ::column-list projection)
    (let [processed-projection (process-projection this (s/conform ::column-list projection))]
      (assoc this :projection processed-projection)))
  (only [this]
    (only this true))
  (only [this is-only]
    (assoc this :only is-only))
  (distinct [this]
    (distinct this true))
  (distinct [this distinct-expression]
    (s/assert ::value-expression distinct-expression)
    (cond
      (boolean? distinct-expression)
      (assoc this :distinct distinct-expression)
      :else
      (let [processed-distinct (resolve-columns this (s/conform ::column-list distinct-expression))]
        (assoc this :distinct processed-distinct))))
  (order-by [this orders]
    (s/assert ::orders orders)
    (let [processed-orders (process-orders this (s/conform ::orders orders))]
      (assoc this :order-by processed-orders)))
  (offset [this offset]
    (assoc this :offset offset))
  (limit [this limit]
    (assoc this :limit limit)))

(defn with-columns [rel spec]
  (let [columns (:columns spec)]
    (reduce
      (fn [acc col]
        (let [id (keyword (gensym "column-"))]
          (-> acc
            (assoc-in [:columns id] {:type :concrete :name col})
            (assoc-in [:column-aliases (->kebab-case-keyword col)] id))))
      rel
      columns)))

(defn with-default-projection [rel]
  (assoc rel :projection (set (keys (:column-aliases rel)))))

(defn spec->relation [spec]
  (-> (->Relation (assoc spec :namespace (->kebab-case-string (:name spec))))
    (with-columns spec)
    (with-default-projection)))