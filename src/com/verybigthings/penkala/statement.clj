(ns com.verybigthings.penkala.statement
  (:require
   [clojure.string :as str]
   [clojure.set :as set]
   [com.verybigthings.penkala.util
    :refer [q
            get-sql-op
            expand-join-path
            path-prefix-join
            vec-conj
            joins
            spc
            join-comma
            join-space
            wrap-parens
            get-rel-alias-with-prefix
            get-rel-alias
            get-relation-name
            get-schema-qualified-relation-name
            make-rel-alias-prefix]]
   [camel-snake-kebab.core
    :refer [->SCREAMING_SNAKE_CASE_STRING
            ->snake_case_string]]
   [com.stuartsierra.dependency :as dep]))

;; This namespace holds everything that's needed to generate an SQL query.
;; Previously this code was split across multiple namespaces, but there are 
;; cases (CTEs) where this organization was becoming impossible, because of 
;; circular dependencies between namespaces.

(def ^:dynamic *scopes* [])
(def ^:dynamic *use-column-db-name-for* #{})
(def ^:dynamic *cte-registry* nil)
(def ^:dynamic *current-cte* [])

(def empty-acc {:query [] :params []})

(declare format-select-query-without-params-resolution)

(defn get-resolved-column-identifier [env rel resolved-col col-def]
  (let [col-id       (:id resolved-col)
        col-rel-path (vec (concat (::join-path-prefix env) (:path resolved-col)))]
    (if (seq col-rel-path)
      (let [col-rel   (get-in rel (expand-join-path col-rel-path))
            col-alias (if (contains? *use-column-db-name-for* (get-in col-rel [:spec :name]))
                        (get-in col-rel [:columns col-id :name])
                        (get-in col-rel [:ids->aliases col-id]))
            full-path (map name (conj col-rel-path col-alias))
            [rel-name & col-parts] full-path]
        (str (q (get-rel-alias-with-prefix env rel-name)) "." (q (path-prefix-join col-parts))))
      (str (q (get-rel-alias-with-prefix env (get-rel-alias rel))) "." (q (:name col-def))))))

(defmulti compile-function-call (fn [_ _ _ function-name _] function-name))
(defmulti compile-value-expression (fn [_ _ _ [vex-type & _]] vex-type))

(defmethod compile-function-call :default [acc env rel function-name args]
  (let [sql-function-name (->snake_case_string function-name)
        last-arg (last args)
        has-order-by (= :order-by-function-argument (first last-arg))

        {args-query :query args-params :params}
        (reduce
         (fn [acc arg]
           (compile-value-expression acc env rel arg))
         empty-acc
         (if has-order-by (butlast args) args))

        {order-by-query :query order-by-params :params}
        (when has-order-by
          (compile-value-expression empty-acc env rel last-arg))

        final-query
        (if has-order-by
          (str sql-function-name (wrap-parens (join-comma args-query) spc (join-space order-by-query)))
          (str sql-function-name (-> args-query join-comma wrap-parens)))]
    (-> acc
        (update :query conj final-query)
        (update :params into args-params)
        (update :params into order-by-params))))

(defmethod compile-function-call :array [acc env rel function-name args]
  (let [{:keys [query params]} (reduce #(compile-value-expression %1 env rel %2) empty-acc args)]
    (-> acc
        (update :params into params)
        (update :query conj (str "ARRAY[" (join-comma query) "]")))))

(defmethod compile-value-expression :default [_ _ _ [vex-type & args]]
  (throw
   (ex-info
    (str "com.verybigthings.penkala.statement.value-expression/compile-value-expression multimethod not implemented for " vex-type)
    {:type vex-type
     :args args})))

(defmethod compile-value-expression nil [acc _ _ _]
  acc)

(defmethod compile-value-expression :function-call [acc env rel [_ {:keys [fn args]}]]
  (compile-function-call acc env rel fn args))

(defmethod compile-value-expression :resolved-column [acc env rel [_ col]]
  (let [col-path (concat (::join-path-prefix env) (:path col))
        col-rel  (if (seq col-path) (get-in rel (expand-join-path col-path)) rel)
        col-def  (get-in col-rel [:columns (:id col)])]
    (case (:type col-def)
      :concrete
      (update acc :query conj (get-resolved-column-identifier env rel col col-def))
      (:computed :aggregate)
      (compile-value-expression acc (if (seq col-path) (assoc env ::join-path-prefix col-path) env) rel (:value-expression col-def)))))

(defmethod compile-value-expression :bare-column-name [acc env rel [_ {:keys [id] :as col}]]
  (update acc :query conj (q (get-in rel [:columns id :name]))))

(defmethod compile-value-expression :value [acc _ _ [_ val]]
  (-> acc
      (update :query conj "?")
      (update :params conj val)))

(defmethod compile-value-expression :keyword [acc _ _ [_ val]]
  (-> acc
      (update :query conj "?")
      (update :params conj (name val))))

(defmethod compile-value-expression :literal [acc _ _ [_ val]]
  (update acc :query conj val))

(defmethod compile-value-expression :unary-operation [{:keys [query params]} env rel [_ {:keys [op arg1]}]]
  (let [sql-op   (get-sql-op op)
        arg1-acc (compile-value-expression empty-acc env rel arg1)]
    {:query (-> query (into (:query arg1-acc)) (conj sql-op))
     :params (-> params (into (:params arg1-acc)))}))

(defmethod compile-value-expression :binary-operation [{:keys [query params]} env rel [_ {:keys [op arg1 arg2]}]]
  (let [sql-op   (get-sql-op op)
        arg1-acc (compile-value-expression empty-acc env rel arg1)
        arg2-acc (compile-value-expression empty-acc env rel arg2)]
    {:query (-> query (into (:query arg1-acc)) (conj sql-op) (into (:query arg2-acc)))
     :params (-> params (into (:params arg1-acc)) (into (:params arg2-acc)))}))

(defmethod compile-value-expression :ternary-operation [{:keys [query params]} env rel [_ {:keys [op arg1 arg2 arg3]}]]
  (let [sql-op (get-sql-op op)
        arg1-acc (compile-value-expression empty-acc env rel arg1)
        arg2-acc (compile-value-expression empty-acc env rel arg2)
        arg3-acc (compile-value-expression empty-acc env rel arg3)]
    {:query (-> query (into (:query arg1-acc)) (conj sql-op) (into (:query arg2-acc)) (conj "AND") (into (:query arg3-acc)))
     :params (-> params (into (:params arg1-acc)) (into (:params arg2-acc)) (into (:params arg3-acc)))}))

(defmethod compile-value-expression :inclusion-operation [{:keys [query params]} env rel [_ {:keys [op column in]}]]
  (let [[in-type in-payload] in
        sql-op   ({:in "IN" :not-in "NOT IN"} op)
        column-acc (compile-value-expression empty-acc env rel column)
        in-acc (if (= :value-expressions in-type)
                 (-> (reduce #(compile-value-expression %1 env rel %2) empty-acc in-payload)
                     (update :query #(wrap-parens (join-comma %))))
                 (-> (compile-value-expression empty-acc env rel in)
                     (update :query #(join-space %))))]
    {:query (-> query (into (:query column-acc)) (conj sql-op) (conj (:query in-acc)))
     :params (-> params (into (:params column-acc)) (into (:params in-acc)))}))

(defmethod compile-value-expression :boolean [acc _ _ [_ value]]
  (update acc :query conj (if value "TRUE" "FALSE")))

(defmethod compile-value-expression :negation [acc env rel [_ {:keys [_ arg1]}]]
  (let [arg1-acc (compile-value-expression empty-acc env rel arg1)]
    (-> acc
        (update :query conj (str "NOT" (-> arg1-acc :query join-space wrap-parens)))
        (update :params into (:params arg1-acc)))))

(defmethod compile-value-expression :connective [acc env rel [_ {:keys [op args]}]]
  (if (= 1 (count args))
    (compile-value-expression acc env rel (first args))
    (let [sql-op (->SCREAMING_SNAKE_CASE_STRING op)
          {:keys [query params]} (reduce
                                  (fn [acc arg]
                                    (-> acc
                                        (compile-value-expression env rel arg)
                                        (update :query conj sql-op)))
                                  empty-acc
                                  args)]
      (-> acc
          (update :params into params)
          (update :query conj (-> query butlast join-space wrap-parens))))))

(defmethod compile-value-expression :param [acc _ rel [_ param-name]]
  (let [param-getter (fn [param-values]
                       (when (not (contains? param-values param-name))
                         (throw (ex-info (str "Missing param " param-name) {:relation rel :param param-name})))
                       (get param-values param-name))]
    (-> acc
        (update :query conj "?")
        (update :params conj param-getter))))

(defmethod compile-value-expression :parent-scope [acc _ rel [_ {:keys [args]}]]
  (let [parent-scope (last *scopes*)]
    (when-not parent-scope
      (throw (ex-info "Parent scope doesn't exist" {:relation rel})))
    (binding [*scopes* (vec (drop-last *scopes*))]
      (let [{:keys [env rel]} parent-scope]
        (reduce
         (fn [acc arg]
           (let [{:keys [query params]} (compile-value-expression acc env rel arg)]
             (-> acc
                 (update :params into params)
                 (update :query into query))))
         acc
         args)))))

(defmethod compile-value-expression :relation [acc env rel [_ inner-rel]]
  (let [rel-alias-prefix (make-rel-alias-prefix env)
        env'             (-> env
                             (dissoc ::join-path-prefix)
                             (update ::relation-alias-prefix vec-conj rel-alias-prefix))
        [query & params] (binding [*scopes* (conj *scopes* {:env env :rel rel})]
                           (format-select-query-without-params-resolution env' (assoc inner-rel :parent rel)))]
    (-> acc
        (update :query conj (wrap-parens query))
        (update :params into params))))

(defmethod compile-value-expression :fragment-literal [acc env rel [_ {:keys [fragment-literal args]}]]
  (reduce
   (fn [acc' arg]
     (let [{:keys [params]} (compile-value-expression empty-acc env rel arg)]
       (update acc' :params into params)))
   (update acc :query conj fragment-literal)
   args))

(defmethod compile-value-expression :fragment-fn [acc env rel [_ {:keys [fragment-fn args]}]]
  (let [compiled-args (mapv
                       (fn [arg]
                         (let [{:keys [query params]} (compile-value-expression empty-acc env rel arg)]
                           (into [(join-space query)] params)))
                       args)
        [query & params] (fragment-fn env rel compiled-args)]
    (-> acc
        (update :query conj query)
        (update :params into params))))

(defmethod compile-value-expression :cast [acc env rel [_ {:keys [value cast-type]}]]
  (let [{:keys [query params]} (compile-value-expression empty-acc env rel value)]
    (-> acc
        (update :params into params)
        (update :query  conj (str "CAST" (wrap-parens (join-space query) spc "AS" spc cast-type))))))

(defn compile-case-value [acc env rel value]
  (if value
    (let [{:keys [query params]} (compile-value-expression empty-acc env rel value)]
      (-> acc
          (update :query into query)
          (update :params into params)))
    acc))

(defn compile-case-whens [acc env rel whens]
  (reduce
   (fn [acc' {:keys [condition then]}]
     (let [{cond-query :query cond-params :params} (compile-value-expression empty-acc env rel condition)
           {then-query :query then-params :params} (compile-value-expression empty-acc env rel then)]
       (-> acc'
           (update :params into cond-params)
           (update :params into then-params)
           (update :query conj "WHEN")
           (update :query into cond-query)
           (update :query conj "THEN")
           (update :query into then-query))))
   acc
   whens))

(defn compile-case-else [acc env rel else]
  (if else
    (let [{:keys [query params]} (compile-value-expression empty-acc env rel else)]
      (-> acc
          (update :query conj "ELSE")
          (update :query into query)
          (update :params into params)))
    acc))

(defmethod compile-value-expression :case [acc env rel [_ {:keys [value whens else]}]]
  (let [{:keys [query params]} (-> empty-acc
                                   (update :query conj "CASE")
                                   (compile-case-value env rel value)
                                   (compile-case-whens env rel whens)
                                   (compile-case-else env rel else)
                                   (update :query conj "END"))]
    (-> acc
        (update :params into params)
        (update :query conj (join-space query)))))

(defmethod compile-value-expression :filter [acc env rel [_ {:keys [agg-vex filter-vex]}]]
  (let [{agg-vex-query :query agg-vex-params :params} (compile-value-expression empty-acc env rel agg-vex)
        {filter-vex-query :query filter-vex-params :params} (compile-value-expression empty-acc env rel filter-vex)]
    (-> acc
        (update :params into agg-vex-params)
        (update :params into filter-vex-params)
        (update :query conj (str (join-space agg-vex-query) spc "FILTER" (wrap-parens "WHERE" spc (join-space filter-vex-query)))))))

(defmethod compile-value-expression :set-operation [acc env rel [_ {:keys [op args]}]]
  (let [sql-op ({:union "UNION"
                 :union-all "UNION ALL"
                 :intersect "INTERSECT"
                 :intersect-all "INTERSECT ALL"
                 :except "EXCEPT"
                 :except-all "EXCEPT ALL"} op)
        qps (reduce
             (fn [acc arg]
               (conj acc (compile-value-expression empty-acc env rel arg)))
             []
             args)
        query (->> qps
                   (map #(join-space (:query %)))
                   (interpose sql-op)
                   (join-space))
        params (into [] (apply concat (map :params qps)))]
    (-> acc
        (update :params into params)
        (update :query conj (wrap-parens query)))))

(defmethod compile-value-expression :grouping-sets [acc env rel [_ grouping-sets]]
  (let [{:keys [query params]}
        (reduce
         (fn [acc grouping-set]
           (if (seq grouping-set)
             (let [{:keys [query params]}
                   (reduce
                    (fn [acc' vex]
                      (compile-value-expression acc' env rel vex))
                    empty-acc
                    grouping-set)]
               (-> acc
                   (update :params into params)
                   (update :query conj (-> query join-comma wrap-parens))))
             (update acc :query conj "()")))
         empty-acc
         grouping-sets)]
    (-> acc
        (update :params into params)
        (update :query conj (str "GROUPING SETS" spc (-> query join-comma wrap-parens))))))

(defmethod compile-value-expression :cube [acc env rel [_ column-list]]
  (let [{:keys [query params]} (reduce #(compile-value-expression %1 env rel %2) empty-acc column-list)]
    (-> acc
        (update :params into params)
        (update :query conj (str "CUBE" spc (-> query join-comma wrap-parens))))))

(defmethod compile-value-expression :rollup [acc env rel [_ column-list]]
  (let [{:keys [query params]} (reduce #(compile-value-expression %1 env rel %2) empty-acc column-list)]
    (-> acc
        (update :params into params)
        (update :query conj (str "ROLLUP" spc (-> query join-comma wrap-parens))))))

(defmethod compile-value-expression :extract [acc env rel [_ {:keys [field value-expression]}]]
  (let [{:keys [query params]} (compile-value-expression empty-acc env rel value-expression)]
    (-> acc
        (update :params into params)
        (update :query conj (str "EXTRACT" (wrap-parens (->SCREAMING_SNAKE_CASE_STRING field) spc "FROM" spc (join-space query)))))))

(declare compile-order-by)

(defmethod compile-value-expression :order-by-function-argument [acc env rel [_ {:keys [order-by]}]]
  (compile-order-by acc env rel order-by))

(defn compile-order-by [acc env rel order-by]
  (let [{:keys [query params]}
        (reduce
         (fn [acc {:keys [column order-direction order-nulls]}]
           (let [{:keys [query params]} (compile-value-expression empty-acc env rel column)
                 order (cond-> query
                         order-direction (conj ({:asc "ASC" :desc "DESC"} order-direction))
                         order-nulls (conj ({:nulls-first "NULLS FIRST" :nulls-last "NULLS LAST"} order-nulls)))]
             (-> acc
                 (update :query conj (join-space order))
                 (update :params into params))))
         empty-acc
         order-by)]
    (-> acc
        (update :params into params)
        (update :query conj (str "ORDER BY" spc (join-comma query))))))

(defn compile-updatable-value [acc env rel value]
  (if (nil? value)
    {:query ["NULL"]}
    (compile-value-expression acc env rel value)))

(defn with-updatable-from [acc env updatable]
  (if-let [from (:joins updatable)]
    (let [{:keys [query params]}
          (reduce-kv
           (fn [acc' alias f]
             (let [from-relation (:relation f)
                   [from-query & from-params] (binding [*scopes* (conj *scopes* {:env env :rel updatable})]
                                                (format-select-query-without-params-resolution env from-relation))
                   from-clause (str (wrap-parens from-query) spc (q (get-rel-alias-with-prefix env alias)))]
               (-> acc'
                   (update :query conj from-clause)
                   (update :params into from-params))))
           empty-acc
           from)]
      (-> acc
          (update :query conj "FROM")
          (update :query conj (join-comma query))
          (update :params into params)))
    acc))

(defn with-updates [acc env updatable]
  (let [{:keys [query params]}
        (reduce-kv
         (fn [acc' col col-update]
           (let [col-id   (get-in updatable [:aliases->ids col])
                 col-name (get-in updatable [:columns col-id :name])
                 {:keys [query params]} (compile-updatable-value empty-acc env updatable col-update)]
             (-> acc'
                 (update :query conj (join-space (into [(q col-name) "="] query)))
                 (update :params into params))))
         empty-acc
         (:updates updatable))]
    (-> acc
        (update :query conj (str "SET" spc (join-comma query)))
        (update :params into params))))

(defn with-on-conflict-where [acc env insertable]
  (if-let [where (get-in insertable [:on-conflict :where])]
    (let [{:keys [query params]}
          (compile-value-expression empty-acc env insertable where)]
      (-> acc
          (update :query conj "WHERE")
          (update :query into query)
          (update :params into params)))
    acc))

(defn with-on-conflict-updates [acc env insertable]
  (if-let [updates (get-in insertable [:on-conflict :updates])]
    (let [{:keys [query params]}
          (reduce-kv
           (fn [acc' col col-update]
             (let [col-id   (get-in insertable [:aliases->ids col])
                   col-name (get-in insertable [:columns col-id :name])
                   {:keys [query params]} (binding [*use-column-db-name-for* (set/union *use-column-db-name-for* #{"EXCLUDED"})]
                                            (compile-updatable-value empty-acc env insertable col-update))]
               (-> acc'
                   (update :query conj (join-space (into [(q col-name) "="] query)))
                   (update :params into params))))
           empty-acc
           updates)]
      (-> acc
          (update :query into ["SET" (join-comma query)])
          (update :params into params)))
    acc))

(defn with-on-conflict-conflict-target [acc env insertable]
  (if-let [[conflict-target-type conflict-target] (get-in insertable [:on-conflict :conflict-target])]
    (case conflict-target-type
      :value-expressions
      (let [{:keys [query params]}
            (reduce
             (fn [acc conflict]
               (let [{:keys [query params]} (compile-value-expression empty-acc env insertable conflict)]
                 (-> acc
                     (update :query conj (join-space query))
                     (update :params into params))))
             empty-acc
             conflict-target)]
        (-> acc
            (update :query conj (-> query join-comma wrap-parens))
            (update :params into params)))
      :on-constraint
      (update acc :query into ["ON CONSTRAINT" conflict-target]))
    acc))

(defn with-on-conflict [acc env insertable]
  (if-let [on-conflict (:on-conflict insertable)]
    (let [action ({:nothing "DO NOTHING" :update "DO UPDATE"} (:action on-conflict))]
      (-> acc
          (update :query conj "ON CONFLICT")
          (with-on-conflict-conflict-target env insertable)
          (with-on-conflict-where env insertable)
          (update :query conj action)
          (with-on-conflict-updates env insertable)))
    acc))

(defn get-insertable-columns [insertable data]
  (let [aliases (-> insertable :aliases->ids keys set)]
    (-> (reduce
         (fn [acc entry]
           (let [entry-keys (-> entry keys set)]
             (set/union acc (set/intersection aliases entry-keys))))
         #{}
         data)
        sort
        vec)))

(defn with-insertable-column-value [acc env insertable col-value]
  (let [{:keys [query params]} (compile-value-expression empty-acc env insertable col-value)]
    (-> acc
        (update :params into params)
        (update :query conj (-> query join-space wrap-parens)))))

(defn with-insertable-values [acc env insertable insertable-columns data]
  (let [{:keys [query params]}
        (reduce
         (fn [acc' entry]
           (let [{:keys [query params]}
                 (reduce
                  (fn [entry-acc col]
                    (let [col-value (get entry col)]
                      (if-not (nil? col-value)
                        (with-insertable-column-value entry-acc env insertable col-value)
                        (update entry-acc :query conj "DEFAULT"))))
                  {:query [] :params []}
                  insertable-columns)]
             (-> acc'
                 (update :query conj (-> query join-comma wrap-parens))
                 (update :params into params))))
         {:query [] :params []}
         data)]
    (-> acc
        (update :query conj (join-comma query))
        (update :params into params))))

(defn with-insertable-columns-and-values [acc env insertable]
  (let [data                     (:inserts insertable)
        data'                    (if (map? data) [data] data)
        insertable-columns       (get-insertable-columns insertable data')
        insertable-columns-names (map
                                  (fn [c]
                                    (let [col-id   (get-in insertable [:aliases->ids c])
                                          col-name (get-in insertable [:columns col-id :name])]
                                      (q col-name)))
                                  insertable-columns)]
    (-> acc
        (update :query conj (-> insertable-columns-names join-comma wrap-parens))
        (update :query conj "VALUES")
        (with-insertable-values env insertable insertable-columns data'))))

(defn with-using [acc env deletable]
  (if-let [using (:joins deletable)]
    (let [{:keys [query params]}
          (reduce-kv
           (fn [acc' alias f]
             (let [from-relation (:relation f)
                   [from-query & from-params] (binding [*scopes* (conj *scopes* {:env env :rel deletable})]
                                                (format-select-query-without-params-resolution env from-relation))
                   from-clause (str (wrap-parens from-query) " " (q (get-rel-alias-with-prefix env alias)))]
               (-> acc'
                   (update :query conj from-clause)
                   (update :params into from-params))))
           empty-acc
           using)]
      (-> acc
          (update :query conj "USING")
          (update :query conj (join-comma query))
          (update :params into params)))
    acc))

(defn with-distinct [acc env rel]
  (if-let [dist (:distinct rel)]
    (if (boolean? dist)
      (update acc :query conj "DISTINCT")
      (let [{:keys [query params]} (reduce #(compile-value-expression %1 env rel %2) empty-acc dist)]
        (-> acc
            (update :params into params)
            (update :query conj (str "DISTINCT ON" (-> query join-comma wrap-parens))))))
    acc))

(defn with-projection
  ([acc env rel]
   (let [{:keys [query params]} (with-projection empty-acc env rel [])]
     (-> acc
         (update :query conj (join-comma query))
         (update :params into params))))
  ([acc env rel path-prefix]
   (let [projection (get-in rel [:projection])
         acc'       (reduce
                     (fn [acc alias]
                       (let [col-id            (get-in rel [:aliases->ids alias])
                             col-def           (get-in rel [:columns col-id])
                             path-prefix-names (mapv name path-prefix)
                             col-path          (conj path-prefix-names (name alias))
                             col-alias         (if (seq path-prefix) (path-prefix-join col-path) (name alias))
                             col-name          (if (seq path-prefix) (path-prefix-join (rest col-path)) (:name col-def))
                             rel-alias         (if (seq path-prefix) (first path-prefix-names) (get-rel-alias rel))
                             col-type          (:type col-def)]
                         (cond

                           (or (seq path-prefix) (= col-type :concrete))
                           (update acc :query conj (str (q (get-rel-alias-with-prefix env rel-alias)) "." (q col-name) " AS " (q col-alias)))

                           (and (not (seq path-prefix)) (contains? #{:aggregate :computed} col-type))
                           (let [{:keys [query params]} (compile-value-expression empty-acc env rel (:value-expression col-def))]
                             (-> acc
                                 (update :params into params)
                                 (update :query conj (str (join-space query) spc "AS" spc (q col-alias)))))

                           (and (not (seq path-prefix)) (= :window col-type))
                           (let [{:keys [value-expression partition-by order-by]} col-def
                                 order-by-query-params (when order-by (compile-order-by empty-acc env rel order-by))
                                 partition-by-query-params (when partition-by (reduce #(compile-value-expression %1 env rel %2) empty-acc partition-by))
                                 {:keys [query params]} (compile-value-expression empty-acc env rel value-expression)]
                             (cond
                               (and partition-by-query-params order-by-query-params)
                               (-> acc
                                   (update :params into params)
                                   (update :params into (:params partition-by-query-params))
                                   (update :params into (:params order-by-query-params))
                                   (update :query conj (join-space
                                                        [(join-space query)
                                                         "OVER"
                                                         (wrap-parens
                                                          "PARTITION BY"
                                                          spc (join-space (:query partition-by-query-params))
                                                          spc (join-space (:query order-by-query-params)))
                                                         "AS" (q col-alias)])))
                               partition-by-query-params
                               (-> acc
                                   (update :params into params)
                                   (update :params into (:params partition-by-query-params))
                                   (update :query conj (join-space
                                                        [(join-space query)
                                                         "OVER"
                                                         (wrap-parens "PARTITION BY" spc (join-space (:query partition-by-query-params)))
                                                         "AS" (q col-alias)])))
                               order-by-query-params
                               (-> acc
                                   (update :params into params)
                                   (update :params into (:params partition-by-query-params))
                                   (update :params into (:params order-by-query-params))
                                   (update :query conj (join-space
                                                        [(join-space query)
                                                         "OVER"
                                                         (-> order-by-query-params :query join-space wrap-parens)
                                                         "AS" (q col-alias)])))
                               :else
                               (-> acc
                                   (update :params into params)
                                   (update :query conj (str (join-space query) spc "OVER () AS" spc (q col-alias)))))))))
                     acc
                     (sort projection))]
     (reduce-kv
      (fn [acc' alias {:keys [relation projection]}]
        (with-projection acc' env (update relation :projection #(or projection %)) (conj path-prefix alias)))
      acc'
      (get-in rel [:joins])))))

(defn with-returning [acc env rel]
  ;; insertable/updateable/deletable might have a from table set which will be reusing the joins map
  ;; and we don't want the infer function to pick it up, so we remove it here
  (if (:projection rel)
    (-> acc
        (update :query conj "RETURNING")
        (with-projection env (dissoc rel :joins)))
    acc))

(defn register-cte! [rel]
  (let [cte-registry @*cte-registry*
        cte-name (get-in rel [:spec :name])
        registered-cte (get cte-registry cte-name)
        registered-ancestors (:ancestors registered-cte)
        parent-cte *current-cte*
        ancestors (cond
                    (and (seq registered-ancestors) parent-cte) (conj registered-ancestors parent-cte)
                    (seq registered-ancestors) registered-ancestors
                    parent-cte #{parent-cte}
                    :else #{})]
    (swap! *cte-registry* assoc cte-name {:cte rel :ancestors ancestors})))

(defn with-selectable-from [acc env rel]
  (let [rel-query (get-in rel [:spec :query])]
    (cond
      (get-in rel [:spec :cte])
      (do
        (register-cte! rel)
        (update acc :query conj (join-space ["FROM" (get-relation-name rel)])))

      rel-query
      (let [rel-alias-prefix (make-rel-alias-prefix env)
            subquery-env     (-> env
                                 (update ::relation-alias-prefix vec-conj rel-alias-prefix))
            [query & params] (if (fn? rel-query)
                               (binding [*scopes* (conj *scopes* {:env env :rel rel})]
                                 (rel-query subquery-env))
                               rel-query)
            rel-name         (get-rel-alias rel)]
        (-> acc
            (update :params into params)
            (update :query conj (join-space ["FROM" query "AS" (q (get-rel-alias-with-prefix env rel-name))]))))

      :else
      (let [rel-name (get-rel-alias rel)]
        (update acc :query into [(if (:only rel) "FROM ONLY" "FROM")
                                 (get-schema-qualified-relation-name env rel)
                                 "AS"
                                 (q (get-rel-alias-with-prefix env rel-name))])))))

(defn with-joins
  ([acc env rel]
   (with-joins acc env rel []))
  ([acc env rel path-prefix]
   (reduce-kv
    (fn [acc' alias j]
      (let [{join-type :type :keys [on relation]} j
            join-sql-type (get joins join-type)
            join-alias    (->> (conj path-prefix alias) (map name) path-prefix-join)
            join-relation (if (contains? #{:left-lateral :right-lateral} join-type)
                            (assoc relation :parent rel)
                            relation)
            [join-query & join-params] (binding [*scopes* (conj *scopes* {:env env :rel rel})]
                                         (format-select-query-without-params-resolution env join-relation))
            join-clause   (cond-> [join-sql-type (wrap-parens join-query) (q (get-rel-alias-with-prefix env join-alias))]
                            (seq on) (conj "ON"))
            {:keys [query params]} (compile-value-expression {:query join-clause :params (vec join-params)} (assoc env ::join-path-prefix path-prefix) rel on)]

        (-> acc'
            (update :params into params)
            (update :query into query))))
    acc
    (get-in rel [:joins]))))

(defn with-where [acc env rel]
  (if-let [where (:where rel)]
    (-> acc
        (update :query conj "WHERE")
        (compile-value-expression env rel where))
    acc))

(defn with-having [acc env rel]
  (if-let [having (:having rel)]
    (-> acc
        (update :query conj "HAVING")
        (compile-value-expression env rel having))
    acc))

(defn any-join-has-projection? [joins]
  (->> joins
       (map (fn [[_ join]]
              (or (-> join :projection seq)
                  (-> join :relation :projection seq)
                  (any-join-has-projection? (get-in join [:relation :joins])))))
       (filter identity)
       first))

(defn rel-should-be-implicitely-grouped-by? [{:keys [projection aliases->ids columns joins]}]
  (let [{:keys [aggregate non-aggregate]}
        (reduce
         (fn [acc alias]
           (let [col-id (get aliases->ids alias)
                 col-type (get-in columns [col-id :type])
                 acc' (if (= :aggregate col-type)
                        (assoc acc :aggregate true)
                        (assoc acc :non-aggregate true))
                 {:keys [aggregate non-aggregate]} acc']
             (if (and aggregate non-aggregate)
               (reduced acc')
               acc')))
         {:aggregate false
          :non-aggregate false}
         projection)]

    ;; This checks if we should add implicit group by clause. This should happen in two cases:
    ;; 1. Relation has both aggregate and non-aggregate columns
    ;; 2. Relation has aggregate column and it has at least one joined relation that has a projection
    (cond
      (and aggregate non-aggregate)
      true

      aggregate
      (any-join-has-projection? joins)

      :else
      false)))

(defn with-implicit-group-by [acc env rel path-prefix]
  (let [projection (get-in rel [:projection])
        acc'       (reduce
                    (fn [acc alias]
                      (let [col-id            (get-in rel [:aliases->ids alias])
                            col-def           (get-in rel [:columns col-id])
                            path-prefix-names (mapv name path-prefix)
                            col-path          (conj path-prefix-names (name alias))
                            col-name          (if (seq path-prefix) (path-prefix-join (rest col-path)) (:name col-def))
                            rel-alias         (if (seq path-prefix) (first path-prefix-names) (get-rel-alias rel))
                            col-type          (:type col-def)]

                        (cond

                          (or (seq path-prefix) (= col-type :concrete))
                          (update acc :query conj (str (q (get-rel-alias-with-prefix env rel-alias)) "." (q col-name)))

                          (and (not (seq path-prefix)) (= :computed col-type))
                          (let [{:keys [query params]} (compile-value-expression empty-acc env rel (:value-expression col-def))]
                            (-> acc
                                (update :params into params)
                                (update :query conj (str (join-space query)))))

                          :else acc)))
                    acc
                    (sort projection))]
    (reduce-kv
     (fn [acc alias {:keys [relation projection]}]
       (let [relation' (update relation :projection #(or projection %))
             path-prefix' (conj path-prefix alias)]
         (with-implicit-group-by acc env relation' path-prefix')))
     acc'
     (get-in rel [:joins]))))

(defn with-explicit-group-by [acc env {:keys [group-by] :as rel}]
  (reduce
   (fn [acc vex]
     (let [[vex-type vex-value] vex]
       ;; If we're referencing a value from joined relation, we just want to place the identifier here
       (if (and (= :resolved-column vex-type) (seq (:path vex-value)))
         (let [col-path (concat (::join-path-prefix env) (:path vex-value))
               col-rel  (if (seq col-path) (get-in rel (expand-join-path col-path)) rel)
               col-def  (get-in col-rel [:columns (:id vex-value)])]
           (update acc :query conj (get-resolved-column-identifier env rel vex-value col-def)))
         (compile-value-expression acc env rel vex))))
   acc
   group-by))

(defn with-group-by [acc env {:keys [group-by] :as rel}]
  (let [{:keys [query params]} (cond
                                 group-by
                                 (with-explicit-group-by empty-acc env rel)

                                 (rel-should-be-implicitely-grouped-by? rel)
                                 (with-implicit-group-by empty-acc env rel [])

                                 :else
                                 empty-acc)]
    (cond-> acc
      (seq query) (update :query conj (str "GROUP BY" spc (join-comma query)))
      (seq params) (update :params into params))))


(defn with-order-by [acc env rel]
  (let [order-by (:order-by rel)]
    (if (seq order-by)
      (compile-order-by acc env rel order-by)
      acc)))

(defn with-lock [acc _ rel]
  (let [{:keys [type rows]} (:lock rel)]
    (cond-> acc
      type (update :query conj (str "FOR" spc (-> type ->SCREAMING_SNAKE_CASE_STRING (str/replace #"_" " "))))
      rows (update :query conj (-> rows ->SCREAMING_SNAKE_CASE_STRING (str/replace #"_" " "))))))

(defn with-offset [acc _ rel]
  (if-let [offset (:offset rel)]
    (update acc :query conj (str "OFFSET" spc offset spc "ROWS"))
    acc))

(defn with-fetch [acc _ rel]
  (if-let [fetch (:fetch rel)]
    (update acc :query conj (str "FETCH NEXT" spc fetch spc "ROWS ONLY"))
    acc))

(defn with-cte-search [env acc cte]
  (if-let [cte-search (get-in cte [:spec :cte :search])]
    (let [{:keys [type column virtual-column]} cte-search
          {col-query :query col-params :params} (compile-value-expression empty-acc env cte column)
          {vcol-query :query vcol-params :params} (compile-value-expression empty-acc env cte virtual-column)
          params (into col-params vcol-params)
          query ["SEARCH" ({:depth-first "DEPTH" :breadth-first "BREADTH"} type) "FIRST BY"
                 (join-space col-query) "SET" (join-space vcol-query)]]
      (-> acc
          (update :params into params)
          (update :query conj (join-space query))))
    acc))

(defn with-cte-cycle [env acc cte]
  (if-let [cte-cycle (get-in cte [:spec :cte :cycle])]
    (let [{:keys [type column virtual-column using-virtual-column]} cte-cycle
          {col-query :query col-params :params} (compile-value-expression empty-acc env cte column)
          {vcol-query :query vcol-params :params} (compile-value-expression empty-acc env cte virtual-column)
          {uvcol-query :query uvcol-params :params} (compile-value-expression empty-acc env cte using-virtual-column)
          params (vec (concat col-params vcol-params uvcol-params))
          query ["CYCLE" (join-space col-query) "SET" (join-space vcol-query) "USING" (join-space uvcol-query)]]
      (-> acc
          (update :params into params)
          (update :query conj (join-space query))))
    acc))

(defn with-cte [env acc cte]
  (let [cte-query (get-in cte [:spec :query])
        materialized? (:materialized? cte-query)
        [query & params] (cte-query env)
        cte-query (cond-> []
                    (get-in cte [:spec :cte :recursive?])
                    (into ["RECURSIVE"])

                    true
                    (into [(q (get-in cte [:spec :name])) "AS"])

                    (true? materialized?)
                    (conj "MATERIALIZED")

                    (false? materialized?)
                    (conj "NOT MATERIALIZED")

                    true
                    (conj (wrap-parens query))

                    true
                    join-space)
        {:keys [query params]} (as-> empty-acc $
                                 (update $ :query conj cte-query)
                                 (update $ :params into params)
                                 (with-cte-search env $ cte)
                                 (with-cte-cycle env $ cte))]
    (-> acc
        (update :query conj (join-space query))
        (update :params into params))))

(defn sort-ctes [ctes-acc]
  (let [g (reduce-kv
           (fn [g cte-name {:keys [ancestors]}]
             (reduce #(dep/depend %1 cte-name %2) g ancestors))
           (dep/graph)
           ctes-acc)
        sorted (vec (reverse (dep/topo-sort g)))
        dep-free (-> (reduce #(dissoc %1 %2) ctes-acc sorted)
                     keys)]
    (into sorted dep-free)))

(defn with-ctes
  ([env acc] (with-ctes env acc {}))
  ([env acc ctes-acc]
   ;; First get current state of registry
   (let [cte-registry @*cte-registry*]
     ;; Reset *cte-registry* to initial value. This way, if 
     ;; there are some values in the registry after we generate
     ;; the current batch, it means that ctes from the current
     ;; batch referenced some other ctes
     (reset! *cte-registry* {})

     (let [ctes-acc' (reduce-kv
                      (fn [acc cte-name {:keys [ancestors cte]}]
                        ;; If we already have this cte in the registry, just add a new ancestor, otherwise
                        ;; compile CTE and add it to the accumulator
                        (if (get acc cte-name)
                          (update-in acc [cte-name :ancestors] set/union ancestors)
                          (binding [*current-cte* cte-name]
                            (assoc acc cte-name (-> (with-cte env empty-acc cte)
                                                    (assoc :ancestors ancestors))))))
                      ctes-acc
                      cte-registry)]
       (if (seq @*cte-registry*)
         (recur env acc ctes-acc')
         (let [ctes-sort-order (sort-ctes ctes-acc')
               {:keys [query params]} (reduce
                                       (fn [acc cte-name]
                                         (let [{:keys [query params]} (get ctes-acc' cte-name)]
                                           (-> acc
                                               (update :query into query)
                                               (update :params into params))))
                                       empty-acc
                                       ctes-sort-order)]

           (cond-> acc
             (seq query) (update :query conj (join-space ["WITH" (join-comma query)]))
             (seq params) (update :params into params))))))))

(defmacro with-cte-registry!
  "Binds a *cte-registry* if it's not bound yet. Otherwise, it leaves the binding in place.
   This ensures that only the entry function will bind the registry, and then process it, since
   format-query functions can be called recursively.
   
   Since we can ancounter an CTE anywhere in the tree, we need a registry, so we could place the CTE 
   queries before the normal query."
  [env & body]
  `(let [cte-registry# *cte-registry*]
     (if cte-registry#
       (do ~@body)
       (binding [*cte-registry* (atom {})]
         (let [[query# & params#] (do ~@body)
               {ctes-query# :query ctes-params# :params} (with-ctes ~env empty-acc)
               final-query# (if (seq ctes-query#)
                              (join-space [(join-space ctes-query#) query#])
                              query#)
               final-params# (concat ctes-params# params#)]
           (into [final-query#] final-params#))))))

(defn make-format-query-with-params-resolution [query-fn]
  (fn [env rel param-values]
    (let [[query & params] (query-fn env rel)
          resolved-params (if param-values (map (fn [p] (if (fn? p) (p param-values) p)) params) params)]
      (into [query] resolved-params))))

(defn format-select-query-without-params-resolution [env rel]
  (with-cte-registry! env
    (let [{:keys [query params]} (-> {:query ["SELECT"] :params []}
                                     (with-distinct env rel)
                                     (with-projection env rel)
                                     (with-selectable-from env rel)
                                     (with-joins env rel)
                                     (with-where env rel)
                                     (with-group-by env rel)
                                     (with-having env rel)
                                     (with-order-by env rel)
                                     (with-lock env rel)
                                     (with-offset env rel)
                                     (with-fetch env rel))]
      (into [(join-space query)] params))))

(def format-select-query (make-format-query-with-params-resolution format-select-query-without-params-resolution))

(defn format-delete-query-without-params-resolution [env deletable]
  (with-cte-registry! env
    (let [{:keys [query params]} (-> {:query [(if (:only deletable) "DELETE FROM ONLY" "DELETE FROM")
                                              (get-schema-qualified-relation-name env deletable)]
                                      :params []}
                                     (with-using env deletable)
                                     (with-where env deletable)
                                     (with-returning env deletable))]
      (into [(join-space query)] params))))

(def format-delete-query (make-format-query-with-params-resolution format-delete-query-without-params-resolution))

(defn format-insert-query [env insertable]
  (with-cte-registry! env
    (let [{:keys [query params]} (-> {:query ["INSERT INTO"
                                              (get-schema-qualified-relation-name env insertable)]
                                      :params []}
                                     (with-insertable-columns-and-values env insertable)
                                     (with-on-conflict env insertable)
                                     (with-returning env insertable))]
      (into [(join-space query)] params))))

(defn format-update-query-without-params-resolution [env updatable]
  (with-cte-registry! env
    (let [{:keys [query params]} (-> {:query [(if (:only updatable) "UPDATE ONLY" "UPDATE")
                                              (get-schema-qualified-relation-name env updatable)]
                                      :params []}
                                     (with-updates env updatable)
                                     (with-updatable-from env updatable)
                                     (with-where env updatable)
                                     (with-returning env updatable))]
      (into [(join-space query)] params))))

(def format-update-query (make-format-query-with-params-resolution format-update-query-without-params-resolution))