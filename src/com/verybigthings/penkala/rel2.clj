(ns com.verybigthings.penkala.rel2
  (:require [clojure.spec.alpha :as s]
            [camel-snake-kebab.core :refer [->kebab-case-keyword]]
            [clojure.string :as str]
            [clojure.walk :refer [prewalk]]
            [com.verybigthings.penkala.util.core :refer [expand-join-path]]))

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

(s/def ::conjunction
  (s/and
    vector?
    (s/cat
      :op #(= :and %)
      :args (s/+ ::value-expression))))

(s/def ::disjunction
  (s/and
    vector?
    (s/cat
      :op #(= :or %)
      :args (s/+ ::value-expression))))

(s/def ::negation
  (s/and
    vector?
    (s/cat
      :op #(= :not %)
      :args (s/+ ::value-expression))))

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

(s/def ::value-expression
  (s/or
    :boolean boolean?
    :keyword keyword?
    :conjunction ::conjunction
    :disjunction ::disjunction
    :negation ::negation
    :unary-operation ::unary-operation
    :binary-operation ::binary-operation
    :ternary-operation ::ternary-operation
    :function-call ::function-call
    :wrapped-column ::wrapped-column
    :wrapped-param ::wrapped-param
    :wrapped-value ::wrapped-value
    :value any?))

(defn resolve-column [rel column]
  (let [column-ns        (namespace column)
        column-join-path (when (seq column-ns) (str/split column-ns #"\."))
        column-name (name column)]
    (println "COLUMN JOIN PATH" (seq column-join-path) (count column-join-path))
    (if (seq column-join-path)
      (let [column-join-path' (map keyword column-join-path)
            column-rel (get-in rel (expand-join-path column-join-path'))
            id (get-in column-rel [:state :column-aliases (keyword column-name)])]
        {:path column-join-path' :id id :name column-name :original column})
      (when-let [id (get-in rel [:state :column-aliases (keyword column-name)])]
        {:id id :name column-name :original column}))))

(defn process-value-expression [rel vex]
  (prewalk
    (fn [node]
      (if (vector? node)
        (let [[node-type & args] node]
          (cond
            (= :wrapped-column node-type)
            (let [column-name (-> args first :subject)
                  column (resolve-column rel column-name)]
              (when (nil? column)
                (throw (ex-info (str "Column " column-name " doesn't exist") {:column column-name :rel rel})))
              [:resolved-column column])

            (= :wrapped-value node-type)
            [:value (-> args :first :subject)]

            (= :wrapped-param node-type)
            [:param (-> args :first :subject)]

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

(defprotocol IRelation
  (join [this join-type join-rel join-alias] [this join-type join-rel join-alias join-on])
  (where [this vex]))

(defrecord Relation [spec state]
  IRelation
  (join [this join-type join-rel join-alias]
    (throw (ex-info "Join without clause not implemented" {})))
  (join [this join-type join-rel join-alias join-on]
    (s/assert ::value-expression join-on)
    (let [with-join (assoc-in this [:state :joins join-alias] {:relation join-rel :type join-type})
          join-on' (process-value-expression with-join (s/conform ::value-expression join-on))]
      (assoc-in with-join [:state :joins join-alias :on] join-on')))
  (where [this where]
    (s/assert ::value-expression where)
    (assoc-in this [:state :where] (process-value-expression this (s/conform ::value-expression where)))))

(defn add-columns [state spec]
  (let [columns (:columns spec)]
    (merge state (reduce
                   (fn [acc col]
                     (let [id (keyword (gensym "column-"))]
                       (-> acc
                         (assoc-in [:columns id] col)
                         (assoc-in [:column-aliases (->kebab-case-keyword col)] id))))
                   {:columns {} :column-aliases {}}
                   columns))))

(defn add-projection [state]
  (assoc state :projection (set (keys (:column-aliases state)))))

(defn spec->relation [spec]
  (->Relation spec
    (-> {}
      (add-columns spec)
      (add-projection))))

(comment
  (def db-spec
    {:products {:schema "public",
                :is_insertable_into true,
                :fk_origin_columns nil,
                :pk ["id"],
                :parent nil,
                :columns
                ["created_at"
                 "description"
                 "id"
                 "in_stock"
                 "name"
                 "price"
                 "specs"
                 "tags"],
                :name "products",
                :fk_dependent_columns nil,
                :fk_origin_schema nil,
                :fk_origin_name nil,
                :fk nil}
     :orders {:schema "public",
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
     :users {:schema "public",
             :is_insertable_into true,
             :fk_origin_columns nil,
             :pk ["Id"],
             :parent nil,
             :columns ["Email" "Id" "Name" "search"],
             :name "Users",
             :fk_dependent_columns nil,
             :fk_origin_schema nil,
             :fk_origin_name nil,
             :fk nil}})

  (s/conform ::value-expression [:and [:or [:= :a :bb (column :foo/bar)]] [:= :foo [:cast :bar/baz :integer]]])

  (let [products (spec->relation (:products db-spec))
        orders   (spec->relation (:orders db-spec))
        users    (spec->relation (:users db-spec))]
    (clojure.pprint/pprint (where users [:= :id 1]))
    #_(println (join products orders :orders [:= :id :orders/product-id]))))