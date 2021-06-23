(ns com.verybigthings.penkala.relation
  (:refer-clojure :exclude [group-by extend distinct])
  (:require [clojure.spec.alpha :as s]
            [camel-snake-kebab.core :refer [->kebab-case-string]]
            [clojure.string :as str]
            [clojure.walk :refer [prewalk]]
            [com.verybigthings.penkala.util
             :refer [expand-join-path path-prefix-join  joins as-vec col->alias]]
            [com.verybigthings.penkala.statement.select :as sel]
            [com.verybigthings.penkala.statement.insert :as ins]
            [com.verybigthings.penkala.statement.update :as upd]
            [com.verybigthings.penkala.statement.delete :as del]
            [clojure.set :as set]))

;; TODO: keyset pagination

(defprotocol IRelation
  (-lock [this lock-type locked-rows])
  (-join [this join-type join-rel join-alias join-on join-projection])
  (-having [this having-expression])
  (-or-having [this having-expression])
  (-offset [this offset])
  (-limit [this limit])
  (-order-by [this orders])
  (-extend [this col-name extend-expression])
  (-extend-with-aggregate [this col-name agg-expression])
  (-extend-with-window [this col-name window-expression partitions orders])
  (-rename [this prev-col-name next-col-name])
  (-select [this projection])
  (-distinct [this distinct-expression])
  (-union [this other-rel])
  (-union-all [this other-rel])
  (-intersect [this other-rel])
  (-except [this other-rel])
  (-wrap [this])
  (-with-parent [this parent]))

(defprotocol IWriteable
  (-returning [this projection]))

(defprotocol IInsertable
  (-with-inserts [this inserts])
  (-on-conflict-do [this action conflicts update where-expression]))

(defprotocol IUpdatable
  (-with-updates [this updates])
  (-from [this from-rel from-alias]))

(defprotocol IDeletable
  (-using [this using-rel using-alias]))

(defprotocol IWhere
  (-where [this where-expression])
  (-or-where [this where-expression]))

(defprotocol IOnly
  (-only [this is-only]))

(defrecord Wrapped [subject-type subject])

(defn ex-info-missing-column [rel node]
  (let [column-name (if (keyword? node) node (:subject node))]
    (ex-info (str "Column " column-name " doesn't exist") {:column column-name :rel rel})))

(def operations
  {:unary #{:is-null :is-not-null
            :is-true :is-not-true
            :is-false :is-not-false
            :is-unknown :is-not-unknown}
   :binary #{:< :> :<= :>= := :<> :!=
             :is-distinct-from :is-not-distinct-from
             :like :ilike :not-like :not-ilike
             :similar-to :not-similar-to
             "~" "~*" "!~" "!~*" "->" "->>" "#>" "#>>"
             "@>" "<@" "?" "?|" "||" "-" "#-" "@?" "@@"
             "&&" "+" "*" "/" "#" "@-@" "##" "<->" ">>"
             "<<" "&<" "&>" "<<|" "|>>" "&<|" "|>&" "<^"
             "^>" "?#" "?-" "?-|" "?||" "~=" "%" "^" "|/"
             "||/" "!" "!!" "@" "&" "|" "<<=" "=>>" "-|-"}
   :ternary #{:between :not-between
              :between-symmetric :not-between-symmetric}})

(def all-operations
  (set/union (:unary operations) (:binary operations) (:ternary operations)))

(s/def ::spec-map map?)

(s/def ::join-type
  #(contains? joins %))

(s/def ::lock-type
  #(contains? #{:share :update :no-key-update :key-share} %))

(s/def ::locked-rows
  #(contains? #{:nowait :skip-locked} %))

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
    :op (s/or
         :operator #(contains? (:unary operations) %)
         :wrapped-operator #(and (= Wrapped (type %)) (= :unary-operator (:subject-type %))))
    :arg1 ::value-expression)))

(s/def ::set-operation
  (s/and
   vector?
   (s/cat
    :op #(contains? #{:union :except :intersect} %)
    :args (s/+ ::value-expression))))

(s/def ::binary-operation
  (s/and
   vector?
   (s/cat
    :op (s/or
         :operator #(contains? (:binary operations) %)
         :wrapped-operator #(and (= Wrapped (type %)) (= :binary-operator (:subject-type %))))
    :arg1 ::value-expression
    :arg2 ::value-expression)))

(s/def ::ternary-operation
  (s/and
   vector?
   (s/cat
    :op (s/or
         :operator #(contains? (:ternary operations) %)
         :wrapped-operator #(and (= Wrapped (type %)) (= :ternary-operator (:subject-type %))))
    :arg1 ::value-expression
    :arg2 ::value-expression
    :arg3 ::value-expression)))

(s/def ::inclusion-operation
  (s/and
   vector?
   (s/cat
    :op #(contains? #{:in :not-in} %)
    :column ::value-expression
    :in (s/or
         :relation ::relation
         :set-operation ::set-operation
         :value-expressions (s/coll-of ::value-expression)))))

(s/def ::cast
  (s/and
   vector?
   (s/cat
    :fn #(= :cast %)
    :value ::value-expression
    :cast-type string?)))

(s/def ::function-call
  (s/and
   vector?
   (s/cat
    :fn #(and (keyword? %) (not (contains? all-operations %)))
    :args (s/+ ::value-expression))))

(s/def ::parent-scope
  (s/and
   vector?
   (s/cat
    :op #(= :parent-scope %)
    :args (s/+ ::value-expression))))

(s/def ::fragment-literal
  (s/and
   vector?
   (s/cat
    :op #(= :fragment %)
    :fragment-literal string?
    :args (s/+ ::value-expression))))

(s/def ::fragment-fn
  (s/and
   vector?
   (s/cat
    :op #(= :fragment %)
    :fragment-fn fn?
    :args (s/+ ::value-expression))))

(s/def ::on-constraint
  (s/tuple #(= :on-constraint %) string?))

(s/def ::value-expressions
  (s/coll-of ::value-expression))

(s/def ::conflict-target
  (s/or
   :on-constraint ::on-constraint
   :value-expressions ::value-expressions))

(s/def ::relation
  #(satisfies? IRelation %))

(s/def ::writeable
  #(satisfies? IWriteable %))

(s/def ::insertable
  #(satisfies? IInsertable %))

(s/def ::updatable
  #(satisfies? IUpdatable %))

(s/def ::deletable
  #(satisfies? IDeletable %))

(s/def ::wrapped-column
  #(and (= Wrapped (type %)) (= :column (:subject-type %))))

(s/def ::wrapped-value
  #(and (= Wrapped (type %)) (= :value (:subject-type %))))

(s/def ::wrapped-param
  #(and (= Wrapped (type %)) (= :param (:subject-type %))))

(s/def ::wrapped-literal
  #(and (= Wrapped (type %)) (= :literal (:subject-type %))))

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

(s/def ::when
  (s/and vector?
         (s/cat
          :when #(= :when %)
          :condition ::value-expression
          :then ::value-expression)))

(s/def ::case
  (s/and vector?
         (s/cat
          :case #(= :case %)
          :value (s/? (s/and
                       #(not (and (vector? %) (= :when (first %))))
                       ::value-expression))
          :whens (s/+ ::when)
          :else (s/? ::value-expression))))

(s/def ::filter
  (s/and vector?
         (s/cat
          :filter #(= :filter %)
          :agg-vex ::value-expression
          :filter-vex ::value-expression)))

(s/def ::updates
  (s/map-of keyword? ::value-expression))

(s/def ::inserts
  (s/map-of keyword? ::value-expression))

(s/def ::orders
  (s/coll-of ::order))

(s/def ::value-expression
  (s/or
   :boolean boolean?
   :keyword keyword?
   :relation ::relation
   :connective ::connective
   :negation ::negation
   :set-operation ::set-operation
   :unary-operation ::unary-operation
   :binary-operation ::binary-operation
   :ternary-operation ::ternary-operation
   :inclusion-operation ::inclusion-operation
   :parent-scope ::parent-scope
   :fragment-fn ::fragment-fn
   :fragment-literal ::fragment-literal
   :cast ::cast
   :case ::case
   :filter ::filter
   :function-call ::function-call
   :wrapped-literal ::wrapped-literal
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
        column-name      (name column)]
    (if (seq column-join-path)
      (let [column-join-path' (map keyword column-join-path)
            column-rel        (get-in rel (expand-join-path column-join-path'))
            id                (get-in column-rel [:aliases->ids (keyword column-name)])
            db-name (get-in column-rel [:columns id :name])]
        (when id
          {:path column-join-path' :id id :name column-name :original column :db-name db-name}))
      (let [id (get-in rel [:aliases->ids (keyword column-name)])
            db-name (get-in rel [:columns id :name])]
        (when id
          {:id id :name column-name :original column :db-name db-name})))))

(defn extract-operator [[op-type operator]]
  (let [extracted-operator (if (= :operator op-type) operator (:subject operator))]
    (if (= :!= extracted-operator)
      :<>
      extracted-operator)))

(defn process-value-expression [rel node]
  (let [[node-type args] node]
    (case node-type
      :wrapped-column
      (let [column (resolve-column rel args)]
        (when (nil? column)
          (throw (ex-info-missing-column rel node)))
        [:resolved-column column])

      :wrapped-value
      [:value (:subject args)]

      :wrapped-param
      [:param (:subject args)]

      :wrapped-literal
      [:literal (:subject args)]

      :keyword
      (let [column (resolve-column rel args)]
        (when (nil? column)
          (throw (ex-info-missing-column rel args)))
        [:resolved-column column])

      (:connective :function-call :fragment-literal :fragment-fn)
      (update-in node [1 :args] (fn [args] (mapv #(process-value-expression rel %) args)))

      :negation
      (update-in node [1 :arg1] #(process-value-expression rel %))

      :cast
      (update-in node [1 :value] #(process-value-expression rel %))

      :case
      (-> node
          (update-in [1 :value] #(when % (process-value-expression rel %)))
          (update-in [1 :whens] (fn [whens]
                                  (mapv (fn [when]
                                          (-> when
                                              (update :condition #(process-value-expression rel %))
                                              (update :then #(process-value-expression rel %))))
                                        whens)))
          (update-in [1 :else] #(when % (process-value-expression rel %))))

      :filter
      (-> node
          (update-in [1 :agg-vex] #(process-value-expression rel %))
          (update-in [1 :filter-vex] #(process-value-expression rel %)))

      :unary-operation
      (-> node
          (update-in [1 :op] extract-operator)
          (update-in [1 :arg1] #(process-value-expression rel %)))

      :binary-operation
      (-> node
          (update-in [1 :op] extract-operator)
          (update-in [1 :arg1] #(process-value-expression rel %))
          (update-in [1 :arg2] #(process-value-expression rel %)))

      :ternary-operation
      (-> node
          (update-in [1 :op] extract-operator)
          (update-in [1 :arg1] #(process-value-expression rel %))
          (update-in [1 :arg2] #(process-value-expression rel %))
          (update-in [1 :arg3] #(process-value-expression rel %)))

      :parent-scope
      (let [parent (:parent rel)]
        (when (nil? parent)
          (throw (ex-info "Parent scope doesn't exist" {:relation rel})))
        (update-in node [1 :args] (fn [args] (mapv #(process-value-expression (:parent rel) %) args))))

      :inclusion-operation
      (let [in  (get-in node [1 :in])
            in' (if (= :value-expressions (first in))
                  (update in 1 (fn [args] (mapv #(process-value-expression rel %) args)))
                  in)]
        (-> node
            (update-in [1 :column] #(process-value-expression rel %))
            (assoc-in [1 :in] in')))
      node)))

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
     (let [node'             (if (= :column-identifier node-type) {:column-identifier node} node)
           column-identifier (-> node' :column-identifier second)
           column            (resolve-column rel column-identifier)]
       (when (nil? column)
         (throw (ex-info-missing-column rel column-identifier)))
       (assoc node' :column [:resolved-column column])))
   orders))

(defn resolve-columns [rel columns]
  (reduce
   (fn [acc [_ node]]
     (let [column (resolve-column rel node)]
       (when (nil? column)
         (throw (ex-info-missing-column rel node)))
       (conj acc [:resolved-column column])))
   []
   columns))

(defn and-predicate [rel predicate-type predicate-expression]
  (s/assert ::value-expression predicate-expression)
  (let [prev-predicate      (get rel predicate-type)
        processed-predicate (process-value-expression rel (s/conform ::value-expression predicate-expression))]
    (if prev-predicate
      (assoc rel predicate-type [:connective {:op :and :args [prev-predicate processed-predicate]}])
      (assoc rel predicate-type processed-predicate))))

(defn or-predicate [rel predicate-type predicate-expression]
  (s/assert ::value-expression predicate-expression)
  (if-let [prev-predicate (get rel predicate-type)]
    (let [processed-predicate (process-value-expression rel (s/conform ::value-expression predicate-expression))]
      (assoc rel predicate-type [:connective {:op :or :args [prev-predicate processed-predicate]}]))
    (and-predicate rel predicate-type predicate-expression)))

(defn get-projected-columns
  ([rel] (get-projected-columns #{} rel []))
  ([acc rel path-prefix]
   (let [acc'
         (reduce
          (fn [acc col]
            (conj acc (path-prefix-join (map name (conj path-prefix col)))))
          acc
          (:projection rel))]
     (reduce-kv
      (fn [acc join-alias join]
        (get-projected-columns acc (:relation join) (conj path-prefix join-alias)))
      acc'
      (:joins rel)))))

(defn get-select-query
  ([rel env]
   (sel/format-query-without-params-resolution env rel))
  ([rel env params]
   (sel/format-query env rel params)))


(defn get-insert-query [insertable env]
  (ins/format-query env insertable))

(defn get-update-query [updatable env params]
  (upd/format-query env updatable params))

(defn get-delete-query [deletable env params]
  (del/format-query env deletable params))

(defn make-combined-relations-spec [operator rel1 rel2]
  (let [rel1-cols (get-projected-columns rel1)
        rel2-cols (get-projected-columns rel2)]
    (when (not= rel1-cols rel2-cols)
      (throw (ex-info (str operator " requires projected columns to match.") {:left-relation rel1 :right-relation rel2})))
    (let [rel1-name (get-in rel1 [:spec :name])
          rel2-name (get-in rel2 [:spec :name])
          rel-name  (if (= rel1-name rel2-name) rel1-name (str rel1-name "__" rel2-name))
          rel1-pk   (get-in rel1 [:spec :pk])
          rel2-pk   (get-in rel2 [:spec :pk])
          query     (fn [env]
                      (let [[query1 & params1] (get-select-query rel1 env)
                            [query2 & params2] (get-select-query rel2 env)]
                        (vec (concat [(str query1 " " operator " " query2)] params1 params2))))]
      {:name rel-name
       :pk (when (= rel1-pk rel2-pk) rel1-pk)
       :columns rel1-cols
       :query query})))

(declare spec->relation)

(defn -writeable-returning [rel projection]
  (if projection
    (let [processed-projection (process-projection rel (s/conform ::column-list projection))]
      (assoc rel :projection processed-projection))
    (assoc rel :projection nil)))

(defn process-on-conflict-column-references [value]
  (prewalk
   (fn [val]
     (if (and (vector? val) (= :resolved-column (first val)))
       [:literal (or (-> val second :db-name)
                     (-> val second :name))]
       val))
   value))

(defn process-conflict-target [insertable conflict-target]
  (let [[conflict-target-type & _] conflict-target]
    (case conflict-target-type
      :value-expressions
      (update conflict-target 1 (fn [v] (mapv #(process-value-expression insertable %) v)))
      :on-constraint
      (last conflict-target)
      conflict-target)))

(defn process-inserts [inserts]
  (reduce-kv
   (fn [m k v]
     (if-not (nil? v)
       (assoc m k (s/conform ::value-expression v))
       (assoc m k v)))
   {}
   inserts))

(defrecord Insertable [spec]
  IWriteable
  (-returning [this projection]
    (-writeable-returning this projection))
  IInsertable
  (-with-inserts [this inserts]
    (let [processed-inserts (if (map? inserts)
                              (process-inserts inserts)
                              (mapv process-inserts inserts))]
      (assoc this :inserts processed-inserts)))
  (-on-conflict-do [this action conflict-target updates where-expression]
    (let [excluded                  (assoc-in this [:spec :name] "EXCLUDED")
          this'                     (dissoc this :joins)
          this''                    (if updates (assoc this' :joins {:excluded {:relation excluded}}) this')
          processed-conflict-target (when conflict-target
                                      (->> conflict-target
                                           (s/conform ::conflict-target)
                                           (process-conflict-target this')
                                           process-on-conflict-column-references))
          processed-updates         (when updates
                                      (->> updates
                                           (s/conform ::updates)
                                           (filter (fn [[k _]] (contains? (:aliases->ids this'') k)))
                                           (map (fn [[k v]] [k (process-value-expression this'' v)]))
                                           (into {})))
          where                     (when where-expression
                                      (-> this'
                                          (process-value-expression (s/conform ::value-expression where-expression))
                                          process-on-conflict-column-references))]
      (when
       (and (= :on-constraint (first processed-conflict-target))
            where)
        (throw (ex-info "ON CONSTRAINT can't be used with a WHERE clause" {:insertable this
                                                                           :where where-expression
                                                                           :conflict-target conflict-target})))

      (assoc this'' :on-conflict {:action action
                                  :conflict-target processed-conflict-target
                                  :updates processed-updates
                                  :where where}))))

(defrecord Updatable [spec]
  IWriteable
  (-returning [this projection]
    (-writeable-returning this projection))
  IWhere
  (-where [this where-expression]
    (and-predicate this :where where-expression))
  (-or-where [this where-expression]
    (or-predicate this :where where-expression))
  IOnly
  (-only [this is-only]
    (assoc this :only is-only))
  IUpdatable
  (-with-updates [this updates]
    (let [processed-updates (->> updates
                                 (s/conform ::updates)
                                 (filter (fn [[k _]] (contains? (:aliases->ids this) k)))
                                 (map (fn [[k v]] [k (process-value-expression this v)]))
                                 (into {}))]
      (assoc this :updates processed-updates)))
  (-from [this from-rel from-alias]
    (assoc-in this [:joins from-alias] {:relation from-rel})))

(defrecord Deletable [spec]
  IWriteable
  (-returning [this projection]
    (-writeable-returning this projection))
  IWhere
  (-where [this where-expression]
    (and-predicate this :where where-expression))
  (-or-where [this where-expression]
    (or-predicate this :where where-expression))
  IOnly
  (-only [this is-only]
    (assoc this :only is-only))
  IDeletable
  (-using [this using-rel using-alias]
    (assoc-in this [:joins using-alias] {:relation using-rel})))

(defn ensure-join-alias-on-projection [join-alias join-projection]
  (let [join-alias-name (name join-alias)]
    (reduce
     (fn [acc col]
       (if (keyword? col)
         (let [col-ns (namespace col)
               col-nss (str/split col-ns #"\.")]
           (when (not= join-alias-name (first col-nss))
             (throw (ex-info "Columns in join projection must be aliased with the join alias" {:join-alias join-alias :column col})))
           (let [col-ns' (->> col-nss rest (str/join "."))
                 col' (keyword col-ns' (name col))]
             (conj acc col')))
         (conj acc col)))
     []
     join-projection)))

(defrecord Relation [spec]
  IRelation
  (-lock [this lock-type locked-rows]
    (assoc this :lock {:type lock-type :rows locked-rows}))
  (-join [this join-type join-rel join-alias join-on join-projection]
    (let [join-rel' (if (contains? #{:left-lateral :right-lateral} join-type)
                      (assoc join-rel :parent this)
                      join-rel)
          processed-join-projection (when join-projection
                                      (process-projection join-rel' (->> join-projection
                                                                         (ensure-join-alias-on-projection join-alias)
                                                                         (s/conform ::column-list))))
          with-join (assoc-in this [:joins join-alias] {:relation join-rel'
                                                        :type join-type
                                                        :projection processed-join-projection})
          join-on'  (process-value-expression with-join (s/conform ::value-expression join-on))]
      (assoc-in with-join [:joins join-alias :on] join-on')))
  (-having [this having-expression]
    (and-predicate this :having having-expression))
  (-or-having [this having-expression]
    (or-predicate this :having having-expression))
  (-rename [this prev-col-name next-col-name]
    (let [id (get-in this [:aliases->ids prev-col-name])]
      (when (nil? id)
        (throw (ex-info-missing-column this prev-col-name)))
      (let [this' (-> this
                      (assoc-in [:ids->aliases id] next-col-name)
                      (update :aliases->ids #(-> % (dissoc prev-col-name) (assoc next-col-name id))))]
        (if (contains? (:projection this') prev-col-name)
          (update this' :projection #(-> % (disj prev-col-name) (conj next-col-name)))
          this'))))
  (-extend [this col-name extend-expression]
    (when (contains? (:aliases->ids this) col-name)
      (throw (ex-info (str "Column " col-name " already-exists") {:column col-name :relation this})))
    (let [processed-extend (process-value-expression this (s/conform ::value-expression extend-expression))
          id               (keyword (gensym "column-"))]
      (-> this
          (assoc-in [:columns id] {:type :computed
                                   :value-expression processed-extend})
          (assoc-in [:ids->aliases id] col-name)
          (assoc-in [:aliases->ids col-name] id)
          (update :projection conj col-name))))
  (-extend-with-aggregate [this col-name agg-expression]
    (when (contains? (:aliases->ids this) col-name)
      (throw (ex-info (str "Column " col-name " already-exists") {:column col-name :relation this})))
    (let [processed-agg (process-value-expression this (s/conform ::value-expression agg-expression))
          id            (keyword (gensym "column-"))]
      (-> this
          (assoc-in [:columns id] {:type :aggregate
                                   :value-expression processed-agg})
          (assoc-in [:ids->aliases id] col-name)
          (assoc-in [:aliases->ids col-name] id)
          (update :projection conj col-name))))
  (-extend-with-window [this col-name window-expression partitions orders]
    (when (contains? (:aliases->ids this) col-name)
      (throw (ex-info (str "Column " col-name " already-exists") {:column col-name :relation this})))
    (let [processed-window     (process-value-expression this [:function-call (s/conform ::function-call window-expression)])
          processed-partitions (when partitions (resolve-columns this (s/conform ::column-list partitions)))
          processed-orders     (when orders (process-orders this (s/conform ::orders orders)))
          id                   (keyword (gensym "column-"))]
      (-> this
          (assoc-in [:columns id] {:type :window
                                   :value-expression processed-window
                                   :partition-by processed-partitions
                                   :order-by processed-orders})
          (assoc-in [:ids->aliases id] col-name)
          (assoc-in [:aliases->ids col-name] id)
          (update :projection conj col-name))))
  (-select [this projection]
    (let [processed-projection (process-projection this (s/conform ::column-list projection))]
      (assoc this :projection processed-projection)))
  (-distinct [this distinct-expression]
    (cond
      (boolean? distinct-expression)
      (assoc this :distinct distinct-expression)
      :else
      (let [processed-distinct (resolve-columns this (s/conform ::column-list distinct-expression))]
        (assoc this :distinct processed-distinct))))
  (-order-by [this orders]
    (let [processed-orders (process-orders this (s/conform ::orders orders))]
      (assoc this :order-by processed-orders)))
  (-offset [this offset]
    (assoc this :offset offset))
  (-limit [this limit]
    (assoc this :limit limit))
  (-union [this other-rel]
    (spec->relation (make-combined-relations-spec "UNION" this other-rel)))
  (-union-all [this other-rel]
    (spec->relation (make-combined-relations-spec "UNION ALL" this other-rel)))
  (-intersect [this other-rel]
    (spec->relation (make-combined-relations-spec "INTERSECT" this other-rel)))
  (-except [this other-rel]
    (spec->relation (make-combined-relations-spec "EXCEPT" this other-rel)))
  (-wrap [this]
    (spec->relation {:name (get-in this [:spec :name])
                     :columns (get-projected-columns this)
                     :query #(get-select-query this %)
                     :wrapped this}))
  (-with-parent [this parent-rel]
    (assoc this :parent parent-rel))
  IOnly
  (-only [this is-only]
    (assoc this :only is-only))
  IWhere
  (-where [this where-expression]
    (and-predicate this :where where-expression))
  (-or-where [this where-expression]
    (or-predicate this :where where-expression)))

(defn lock
  "Lock selected rows"
  ([rel lock-type]
   (-lock rel lock-type nil))
  ([rel lock-type locked-rows]
   (-lock rel lock-type locked-rows)))

(s/fdef lock
  :args (s/cat
         :rel ::relation
         :lock-type ::lock-type
         :locked-rows (s/? ::locked-rows))
  :ret ::relation)

(defn join
  "Joins another relation. Supported join types are:

  - :inner
  - :inner-lateral
  - :left
  - :left-lateral
  - :right
  - :full
  - :cross

  When joining a relation, columns from the joined relation must be referenced with a namespaced key. Key's namespace
  will be the join alias (e.g. :join-alias/column). If you need to reference a column that's deeply joined, you
  can use join aliases concatenated with a dot (e.g. :join-alias.another-join-alias/column)

  `join-on` can be any value expression, so you can have as complex join predicates as you need."
  ([rel join-type join-rel join-alias join-on]
   (-join rel join-type join-rel join-alias join-on nil))
  ([rel join-type join-rel join-alias join-on join-projection]
   (-join rel join-type join-rel join-alias join-on join-projection)))

(s/fdef join
  :args (s/cat
         :rel ::relation
         :join-type ::join-type
         :join-rel ::relation
         :join-alias keyword?
         :join-on ::value-expression
         :join-projection (s/? ::column-list))
  :ret ::relation)

(defn where
  "And where operation. If there's already a where clause set, this clause will be joined with AND. Accepts a value
   expression which can be nested as needed. You can reference a column in a joined relation by using namespaced keys:

   ```
   (-> beta
      (r/join :inner alpha :alpha [:= :alpha-id :alpha/id])
      (r/where [:= :alpha/id 3]))
   ```

   In this example `:alpha/id` is referencing the `:id` column in the relation joined with the `:alpha` alias."
  [rel where-expression]
  (-where rel where-expression))

(s/fdef where
  :args (s/cat
         :rel ::relation
         :where-expression ::value-expression)
  :ret ::relation)

(defn or-where
  "Or where operation. If there's already a where clause set, this clause will be joined with OR"
  [rel where-expression]
  (-or-where rel where-expression))

(s/fdef or-where
  :args (s/cat
         :rel ::relation
         :where-expression ::value-expression)
  :ret ::relation)

(defn having
  "And having operation. If there's already a having clause set, this clause will be joined with AND"
  [rel having-expression]
  (-having rel having-expression))

(s/fdef having
  :args (s/cat
         :rel ::relation
         :having-expression ::value-expression)
  :ret ::relation)

(defn or-having
  "Or having operation. If there's already a having clause set, this clause will be joined with OR"
  [rel having-expression]
  (-or-having rel having-expression))

(s/fdef or-having
  :args (s/cat
         :rel ::relation
         :having-expression ::value-expression)
  :ret ::relation)

(defn offset
  "Sets the offset parameter"
  [rel offset]
  (-offset rel offset))

(s/fdef offset
  :args (s/cat
         :rel ::relation
         :offset int?)
  :ret ::relation)

(defn limit
  "Sets the limit parameter."
  [rel limit]
  (-limit rel limit))

(s/fdef limit
  :args (s/cat
         :rel ::relation
         :limit int?)
  :ret ::relation)

(defn order-by
  "Sets order by clause. It accepts a vector of columns by which the order will be performed. Columns can be either a
  keyword, or vectors if you need to use descending order or you want to set order for null values:

  - `(order-by rel [:id])`
  - `(order-by rel [[:id :desc]])`
  - `(order-by rel [[:id :desc :nulls-first]])`

  You can reference columns in joined relations by using namespaced keys"
  [rel orders]
  (-order-by rel orders))

(s/fdef order-by
  :args (s/cat
         :rel ::relation
         :orders ::orders)
  :ret ::relation)

(defn extend
  "Extends a relation with a computed column. This column will be automatically selected. Expression can reference
  previously extended columns by name:

  ```
  (-> rel
    (extend :upper-name [:upper :name])
    (extend :lower-upper-name [:lower :upper-name]))
  ```

  You can use reference these columns in any other value expression, and Penkala will correctly compile them in the
  generated SQL.

  ```
  (-> rel
    (extend :upper-name [:upper :name])
    (extend :lower-upper-name [:lower :upper-name])
    (where [:= :lower-upper-name \"FOO\"]))
  ```
  "
  [rel col-name extend-expression]
  (-extend rel col-name extend-expression))

(s/fdef extend
  :args (s/cat
         :rel ::relation
         :col-name keyword?
         :extend-expression ::value-expression)
  :ret ::relation)

(defn extend-with-aggregate
  "Extends the relation with a computed column that is an aggregate value (e.g. sum, max, min...). Root value expression
  must be a function. Penkala doesn't have any explicit support for the built in aggregate functions which means you can
  use whatever your DB supports, including custom aggregate functions.

  If an aggregate column is selected, Penkala will automatically add a GROUP BY clause to the generated SQL.

  ```
  (extend-with-aggregate rel :count [:count 1])
  ```

  This column will be automatically selected."
  ([rel col-name agg-expression]
   (-extend-with-aggregate rel col-name agg-expression)))

(s/fdef extend-with-aggregate
  :args (s/cat
         :rel ::relation
         :col-name keyword?
         :agg-expression ::function-call)
  :ret ::relation)

(defn extend-with-window
  "Extends a relation with a window function column."
  ([rel col-name window-expression]
   (-extend-with-window rel col-name window-expression nil nil))
  ([rel col-name window-expression partitions]
   (-extend-with-window rel col-name window-expression partitions nil))
  ([rel col-name window-expression partitions orders]
   (-extend-with-window rel col-name window-expression partitions orders)))

(s/fdef extend-with-window
  :args (s/cat
         :rel ::relation
         :col-name keyword?
         :window-expression ::function-call
         :partitions (s/? ::column-list)
         :orders (s/? ::orders))
  :ret ::relation)

(defn rename
  "Renames a column. If you rename a column, you must use a new name to reference it after that

  ```
  (-> rel
    (rename :name :product-name)
    (where [:= :product-name \"FOO\"]))
  ```"
  [rel prev-col-name next-col-name]
  (-rename rel prev-col-name next-col-name))

(s/fdef rename
  :args (s/cat
         :rel ::relation
         :prev-col-name keyword?
         :next-col-name keyword?)
  :ret ::relation)

(defn select
  "Selects columns from the relation. You can reference any extended columns here."
  [rel projection]
  (-select rel projection))

(s/fdef select
  :args (s/cat
         :rel ::relation
         :projection ::column-list)
  :ret ::relation)

(defn returning
  "Selects columns from the write operation (insert, update, delete)"
  [writeable projection]
  (-returning writeable projection))

(s/fdef returning
  :args (s/cat
         :rel ::writeable
         :projection ::column-list)
  :ret ::writeable)

(defn distinct
  "Adds a distinct or distinct on clause."
  ([rel]
   (-distinct rel true))
  ([rel distinct-expression]
   (-distinct rel distinct-expression)))

(s/fdef distinct
  :args (s/cat
         :rel ::relation
         :distinct-expression (s/or
                               :boolean boolean?
                               :distinct-expression ::column-list))
  :ret ::relation)

(defn only
  "Adds the only clause to limit the inheritance."
  ([rel]
   (-only rel true))
  ([rel is-only]
   (-only rel is-only)))

(s/fdef only
  :args (s/cat
         :rel ::relation
         :is-only (s/? boolean?))
  :ret ::relation)

(defn union
  "Creates a relation that is a combination of two relations with the UNION operator"
  [rel other-rel]
  (-union rel other-rel))

(s/fdef union
  :args (s/cat
         :rel ::relation
         :other-rel ::relation)
  :ret ::relation)

(defn union-all
  "Creates a relation that is a combination of two relations with the UNION ALL operator"
  [rel other-rel]
  (-union-all rel other-rel))

(s/fdef union-all
  :args (s/cat
         :rel ::relation
         :other-rel ::relation)
  :ret ::relation)

(defn intersect
  "Creates a relation that is a combination of two relations with the INTERSECT operator"
  [rel other-rel]
  (-intersect rel other-rel))

(s/fdef intersect
  :args (s/cat
         :rel ::relation
         :other-rel ::relation)
  :ret ::relation)

(defn except
  "Creates a relation that is a combination of two relations with the EXCEPT operator"
  [rel other-rel]
  (-except rel other-rel))

(s/fdef except
  :args (s/cat
         :rel ::relation
         :other-rel ::relation)
  :ret ::relation)

(defn wrap
  "Wraps a relation, so the original structure is not visible to other relations. Use this in rare cases where you
  want to force a sub-select wrap around the relation. There are very rare cases when this is needed, but for instance
  if you want to have a relation that is joined with other relations, and you want to select only one field, you will
  need to explicitly wrap that relation."
  [rel]
  (-wrap rel))

(s/fdef wrap
  :args (s/cat :rel ::relation)
  :ret ::relation)

(defn with-parent [rel parent]
  (-with-parent rel parent))

(s/fdef with-parent
  :args (s/cat :rel ::relation)
  :ret ::relation)

(defn with-default-columns [rel]
  (let [columns (get-in rel [:spec :columns])]
    (reduce
     (fn [acc col]
       (let [id    (keyword (gensym "column-"))
             alias (col->alias col)]
         (-> acc
             (assoc-in [:columns id] {:type :concrete :name col})
             (assoc-in [:ids->aliases id] alias)
             (assoc-in [:aliases->ids alias] id))))
     rel
     columns)))

(s/fdef with-default-columns
  :args (s/cat :rel ::relation)
  :ret ::relation)

(defn with-default-projection [rel]
  (assoc rel :projection (set (keys (:aliases->ids rel)))))

(s/fdef with-default-projection
  :args (s/cat :rel ::relation)
  :ret ::relation)

(defn with-default-pk [rel]
  (let [pk         (as-vec (get-in rel [:spec :pk]))
        pk-aliases (map col->alias pk)]
    (assoc rel :pk (mapv #(get-in rel [:aliases->ids %]) pk-aliases))))

(s/fdef with-default-pk
  :args (s/cat :rel ::relation)
  :ret ::relation)

(defn with-pk [rel pk-cols]
  (let [pk (reduce
            (fn [acc col]
              (let [col-id (get-in rel [:aliases->ids col])]
                (if col-id
                  (conj acc col-id)
                  (throw (ex-info-missing-column rel col)))))
            []
            pk-cols)]
    (assoc rel :pk pk)))

(s/fdef with-pk
  :args (s/cat :rel ::relation :pk (s/coll-of keyword? :kind vector?))
  :ret ::relation)

(defn with-updates [updatable updates]
  (-with-updates updatable updates))

(s/fdef with-updates
  :args (s/cat :updatable ::updatable :updates ::updates)
  :ret ::updatable)

(defn from [updatable from-rel from-alias]
  (-from updatable from-rel from-alias))

(s/fdef from
  :args (s/cat :updatable ::updatable
               :from-rel ::relation
               :from-alias keyword?)
  :ret ::updatable)

(defn using [deletable using-rel using-alias]
  (-using deletable using-rel using-alias))

(s/fdef using
  :args (s/cat :deletable ::deletable
               :using-rel ::relation
               :using-alias keyword?)
  :ret ::deletable)

(defn with-inserts [insertable inserts]
  (-with-inserts insertable inserts))

(s/fdef with-inserts
  :args (s/cat :insertable ::insertable :inserts ::inserts)
  :ret ::insertable)

(defn on-conflict-do-nothing
  ([insertable] (on-conflict-do-nothing insertable nil nil))
  ([insertable conflicts] (on-conflict-do-nothing insertable conflicts nil))
  ([insertable conflicts where-expression]
   (-on-conflict-do insertable :nothing conflicts nil where-expression)))

(s/fdef on-conflict-do-nothing
  :args (s/cat :insertable ::insertable
               :conflict-target ::conflict-target
               :where-expression (s/? ::value-expression))
  :ret ::insertable)

(defn on-conflict-do-update
  ([insertable conflicts updates] (on-conflict-do-update insertable conflicts updates nil))
  ([insertable conflicts updates where-expression]
   (-on-conflict-do insertable :update conflicts updates where-expression)))

(s/fdef on-conflict-do-update
  :args (s/cat :insertable ::insertable
               :conflict-target ::conflict-target
               :updates ::updates
               :where-expression (s/? ::value-expression))
  :ret ::insertable)

(defn spec->relation [spec-map]
  (-> (->Relation (assoc spec-map :namespace (->kebab-case-string (:name spec-map))))
      with-default-columns
      with-default-projection
      with-default-pk))

(s/fdef spec->relation
  :args (s/cat :spec-map ::spec-map)
  :ret ::relation)

(defn ->writeable [constructor rel]
  (when (nil? rel)
    (throw (ex-info "Invalid relation" {:relation rel})))
  (if (get-in rel [:spec :is-insertable-into])
    (-> (constructor (:spec rel))
        with-default-columns
        with-default-projection
        with-default-pk)
    (throw (ex-info "Relation is not writeable" {:relation rel}))))

(defn ->insertable
  "Converts a relation into an \"insertable\" record which can be used to compose an insert query"
  [rel]
  (->writeable ->Insertable rel))

(s/fdef ->insertable
  :args (s/cat :relation ::relation)
  :ret ::insertable)

(defn ->updatable
  "Converts a relation into an \"updatable\" record which can be used to compose an update query"
  [rel]
  (->writeable ->Updatable rel))

(s/fdef ->updatable
  :args (s/cat :relation ::relation)
  :ret ::updatable)

(defn ->deletable
  "Converts a relation into an \"deletable\" record which can be used to compose a delete query"
  [rel]
  (->writeable ->Deletable rel))

(s/fdef ->deletable
  :args (s/cat :relation ::relation)
  :ret ::deletable)