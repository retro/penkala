(ns com.verybigthings.penkala.statement.select
  (:require [clojure.string :as str]
            [com.verybigthings.penkala.util :refer [q expand-join-path path-prefix-join  vec-conj joins]]
            [com.verybigthings.penkala.statement.shared
             :refer [get-rel-alias-with-prefix
                     get-rel-alias
                     get-schema-qualified-relation-name
                     make-rel-alias-prefix]]
            [camel-snake-kebab.core :refer [->SCREAMING_SNAKE_CASE_STRING ->snake_case_string]]))

(def ^:dynamic *scopes* [])
(def ^:dynamic *use-column-db-name-for* #{})

(def op->sql-op {})

(defn get-sql-op [op]
  (if (string? op)
    op
    (or (op->sql-op op) (-> op ->SCREAMING_SNAKE_CASE_STRING (str/replace #"_" " ")))))

(declare format-query-without-params-resolution)

(def empty-acc {:query [] :params []})

(defn empty-acc? [{:keys [query params]}]
  (and (empty? query) (empty? params)))

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
        {:keys [query params]} (reduce
                                (fn [acc arg]
                                  (compile-value-expression acc env rel arg))
                                empty-acc
                                args)]
    (-> acc
        (update :query conj (str sql-function-name "(" (str/join ", " query) ")"))
        (update :params into params))))

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
                     (update :query #(str "(" (str/join ", " %) ")")))
                 (-> (compile-value-expression empty-acc env rel in)
                     (update :query #(str/join " " %))))]
    {:query (-> query (into (:query column-acc)) (conj sql-op) (conj (:query in-acc)))
     :params (-> params (into (:params column-acc)) (into (:params in-acc)))}))

(defmethod compile-value-expression :boolean [acc _ _ [_ value]]
  (update acc :query conj (if value "TRUE" "FALSE")))

(defmethod compile-value-expression :negation [acc env rel [_ {:keys [_ arg1]}]]
  (let [arg1-acc (compile-value-expression empty-acc env rel arg1)]
    (-> acc
        (update :query conj (str "NOT(" (str/join " " (:query arg1-acc)) ")"))
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
          (update :query conj (str "(" (str/join " " (butlast query)) ")"))))))

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
                           (format-query-without-params-resolution env' (assoc inner-rel :parent rel)))]
    (-> acc
        (update :query conj (str "(" query ")"))
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
                           (into [(str/join " " query)] params)))
                       args)
        [query & params] (fragment-fn env rel compiled-args)]
    (-> acc
        (update :query conj query)
        (update :params into params))))

(defmethod compile-value-expression :cast [acc env rel [_ {:keys [value cast-type]}]]
  (let [{:keys [query params]} (compile-value-expression empty-acc env rel value)]
    (-> acc
        (update :params into params)
        (update :query  conj (str "CAST(" (str/join " " query) " AS " cast-type ")")))))

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
        (update :query conj (str/join " " query)))))

(defmethod compile-value-expression :filter [acc env rel [_ {:keys [agg-vex filter-vex]}]]
  (let [{agg-vex-query :query agg-vex-params :params} (compile-value-expression empty-acc env rel agg-vex)
        {filter-vex-query :query filter-vex-params :params} (compile-value-expression empty-acc env rel filter-vex)]
    (-> acc
        (update :params into agg-vex-params)
        (update :params into filter-vex-params)
        (update :query conj (str (str/join " " agg-vex-query) " FILTER(WHERE " (str/join " " filter-vex-query) ")")))))

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
                   (map #(str/join " " (:query %)))
                   (interpose sql-op)
                   (str/join " "))
        params (into [] (apply concat (map :params qps)))]
    (-> acc
        (update :params into params)
        (update :query conj (str "(" query ")")))))

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
                   (update :query conj (str "(" (str/join ", " query) ")"))))
             (update acc :query conj "()")))
         empty-acc
         grouping-sets)]
    (-> acc
        (update :params into params)
        (update :query conj (str "GROUPING SETS (" (str/join ", " query) ")")))))

(defmethod compile-value-expression :cube [acc env rel [_ column-list]]
  (let [{:keys [query params]} (reduce #(compile-value-expression %1 env rel %2) empty-acc column-list)]
    (-> acc
        (update :params into params)
        (update :query conj (str "CUBE (" (str/join ", " query) ")")))))

(defmethod compile-value-expression :rollup [acc env rel [_ column-list]]
  (let [{:keys [query params]} (reduce #(compile-value-expression %1 env rel %2) empty-acc column-list)]
    (-> acc
        (update :params into params)
        (update :query conj (str "ROLLUP (" (str/join ", " query) ")")))))

(defmethod compile-value-expression :extract [acc env rel [_ {:keys [field value-expression]}]]
  (let [{:keys [query params]} (compile-value-expression empty-acc env rel value-expression)]
    (-> acc
        (update :params into params)
        (update :query conj (str "EXTRACT(" (->SCREAMING_SNAKE_CASE_STRING field) " FROM " (str/join " " query) ")")))))

(defn compile-order-by [acc env rel order-by]
  (let [{:keys [query params]}
        (reduce
         (fn [acc {:keys [column order-direction order-nulls]}]
           (let [{:keys [query params]} (compile-value-expression empty-acc env rel column)
                 order (cond-> query
                         order-direction (conj ({:asc "ASC" :desc "DESC"} order-direction))
                         order-nulls (conj ({:nulls-first "NULLS FIRST" :nulls-last "NULLS LAST"} order-nulls)))]
             (-> acc
                 (update :query conj (str/join " " order))
                 (update :params into params))))
         empty-acc
         order-by)]
    (-> acc
        (update :params into params)
        (update :query conj (str "ORDER BY " (str/join ", " query))))))

(defn with-distinct [acc env rel]
  (if-let [dist (:distinct rel)]
    (if (boolean? dist)
      (update acc :query conj "DISTINCT")
      (let [{:keys [query params]} (reduce #(compile-value-expression %1 env rel %2) empty-acc dist)]
        (-> acc
            (update :params into params)
            (update :query conj (str "DISTINCT ON(" (str/join ", " query) ")")))))
    acc))

(defn with-projection
  ([acc env rel]
   (let [{:keys [query params]} (with-projection empty-acc env rel [])]
     (-> acc
         (update :query conj (str/join ", " query))
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
                                 (update :query conj (str (str/join " " query) " AS " (q col-alias)))))

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
                                   (update :query conj (str/join " "
                                                                 [(str/join " " query)
                                                                  "OVER"
                                                                  (str "("
                                                                       "PARTITION BY " (str/join " " (:query partition-by-query-params))
                                                                       " "
                                                                       (str/join " " (:query order-by-query-params))
                                                                       ")")
                                                                  "AS" (q col-alias)])))
                               partition-by-query-params
                               (-> acc
                                   (update :params into params)
                                   (update :params into (:params partition-by-query-params))
                                   (update :query conj (str/join " "
                                                                 [(str/join " " query)
                                                                  "OVER"
                                                                  (str "(" "PARTITION BY " (str/join " " (:query partition-by-query-params)) ")")
                                                                  "AS" (q col-alias)])))
                               order-by-query-params
                               (-> acc
                                   (update :params into params)
                                   (update :params into (:params partition-by-query-params))
                                   (update :params into (:params order-by-query-params))
                                   (update :query conj (str/join " "
                                                                 [(str/join " " query)
                                                                  "OVER"
                                                                  (str "(" (str/join " " (:query order-by-query-params)) ")")
                                                                  "AS" (q col-alias)])))
                               :else
                               (-> acc
                                   (update :params into params)
                                   (update :query conj (str (str/join " " query) " OVER () AS " (q col-alias)))))))))
                     acc
                     (sort projection))]
     (reduce-kv
      (fn [acc' alias {:keys [relation projection]}]
        (with-projection acc' env (update relation :projection #(or projection %)) (conj path-prefix alias)))
      acc'
      (get-in rel [:joins])))))

(defn with-from [acc env rel]
  (if-let [rel-query (get-in rel [:spec :query])]
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
          (update :query into ["FROM" (str "(" query ")") "AS" (q (get-rel-alias-with-prefix env rel-name))])))

    (let [rel-name (get-rel-alias rel)]
      (update acc :query into [(if (:only rel) "FROM ONLY" "FROM")
                               (get-schema-qualified-relation-name env rel)
                               "AS"
                               (q (get-rel-alias-with-prefix env rel-name))]))))

(defn with-joins
  ([acc env rel] (with-joins acc env rel []))
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
                                         (format-query-without-params-resolution env join-relation))
            join-clause   (cond-> [join-sql-type (str "(" join-query ")") (q (get-rel-alias-with-prefix env join-alias))]
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
    ;;
    ;; We don't have to recursively check because joins will be wrapped in a subselect, so joins of joins
    ;; are invisible from this perspective
    (cond
      (and aggregate non-aggregate)
      true

      aggregate
      (->> joins
           (map (fn [[_ join]]
                  (or (-> join :projection seq)
                      (-> join :relation :projection seq))))
           (filter identity)
           first)

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
                                (update :query conj (str (str/join " " query)))))

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
      (seq query) (update :query conj (str "GROUP BY " (str/join ", " query)))
      (seq params) (update :params into params))))


(defn with-order-by [acc env rel]
  (let [order-by (:order-by rel)]
    (if (seq order-by)
      (compile-order-by acc env rel order-by)
      acc)))

(defn with-lock [acc _ rel]
  (let [{:keys [type rows]} (:lock rel)]
    (cond-> acc
      type (update :query conj (str "FOR " (-> type ->SCREAMING_SNAKE_CASE_STRING (str/replace #"_" " "))))
      rows (update :query conj (-> rows ->SCREAMING_SNAKE_CASE_STRING (str/replace #"_" " "))))))

(defn with-offset [acc _ rel]
  (if-let [offset (:offset rel)]
    (update acc :query conj (str "OFFSET " offset " ROWS"))
    acc))

(defn with-fetch [acc _ rel]
  (if-let [fetch (:fetch rel)]
    (update acc :query conj (str "FETCH NEXT " fetch " ROWS ONLY"))
    acc))

(defn format-query-without-params-resolution [env rel]
  (let [{:keys [query params]} (-> {:query ["SELECT"] :params []}
                                   (with-distinct env rel)
                                   (with-projection env rel)
                                   (with-from env rel)
                                   (with-joins env rel)
                                   (with-where env rel)
                                   (with-group-by env rel)
                                   (with-having env rel)
                                   (with-order-by env rel)
                                   (with-lock env rel)
                                   (with-offset env rel)
                                   (with-fetch env rel))]
    (into [(str/join " " query)] params)))

(defn format-query [env rel param-values]
  (let [[query & params] (format-query-without-params-resolution env rel)
        resolved-params (if param-values (map (fn [p] (if (fn? p) (p param-values) p)) params) params)]
    (into [query] resolved-params)))