(ns com.verybigthings.penkala.statement.select2
  (:require [com.verybigthings.penkala.statement.value-expression :refer [compile-value-expression]]
            [clojure.string :as str]
            [com.verybigthings.penkala.util.core :refer [q path-prefix-join join-separator]]
            [camel-snake-kebab.core :refer [->SCREAMING_SNAKE_CASE_STRING]]))

(declare format-query-without-params-resolution)

(def empty-acc {:query [] :params []})

(defn schema-qualified-relation-name [env rel]
  (let [rel-name   (get-in rel [:spec :name])
        rel-schema (get-in rel [:spec :schema])]
    (str (q rel-schema) "." (q rel-name))))

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
                        (let [col-id    (get-in rel [:aliases->ids alias])
                              col-def   (get-in rel [:columns col-id])
                              path-prefix-names (mapv name path-prefix)
                              col-path  (conj path-prefix-names (name alias))
                              col-alias (if (seq path-prefix) (path-prefix-join col-path) (name alias))
                              col-name (if (seq path-prefix) (path-prefix-join (rest col-path)) (:name col-def))
                              rel-alias (if (seq path-prefix) (first path-prefix-names) (get-in rel [:spec :name]))
                              col-type (:type col-def)]
                          (cond

                            (or (seq path-prefix) (= col-type :concrete))
                            (update acc :query conj (str (q rel-alias) "." (q col-name) " AS " (q col-alias)))

                            (and (not (seq path-prefix)) (contains? #{:computed :aggregate} col-type))
                            (let [{:keys [query params]} (compile-value-expression empty-acc env rel (:value-expression col-def))]
                              (-> acc
                                (update :params into params)
                                (update :query conj (str (str/join " " query) " AS " (q col-alias))))))))
                      acc
                      (sort projection))]
     (reduce-kv
       (fn [acc' alias {:keys [relation]}]
         (with-projection acc' env relation (conj path-prefix alias)))
       acc'
       (get-in rel [:joins])))))

(defn with-from [acc env rel]
  (if-let [rel-query (get-in rel [:spec :query])]
    (let [[query & params] (if (fn? rel-query) (rel-query env) rel-query)
          rel-name (get-in rel [:spec :name])]
      (-> acc
        (update :params into params)
        (update :query into ["FROM" (str "(" query ")") "AS" (q rel-name)])))

    (let [rel-name (get-in rel [:spec :name])]
      (update acc :query into [(if (:only rel) "FROM ONLY" "FROM")
                               (schema-qualified-relation-name env rel)
                               "AS"
                               (q rel-name)]))))

(defn with-joins
  ([acc env rel] (with-joins acc env rel []))
  ([acc env rel path-prefix]
   (reduce-kv
     (fn [acc' alias j]
       (let [join-sql-type ({:left "LEFT OUTER" :inner "INNER"} (:type j))
             join-alias    (->> (conj path-prefix alias) (map name) path-prefix-join)
             join-relation (:relation j)
             [join-query & join-params] (format-query-without-params-resolution env join-relation)
             join-clause   [join-sql-type "JOIN" (str "(" join-query ")") (q join-alias) "ON"]
             {:keys [query params]} (compile-value-expression {:query join-clause :params (vec join-params)} (assoc env :join/path-prefix path-prefix) rel (:on j))]
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

(defn with-group-by-and-having
  ([acc env rel]
   (let [has-group-by (reduce
                        (fn [_ alias]
                          (let [col-id  (get-in rel [:aliases->ids alias])
                                col-def (get-in rel [:columns col-id])]
                            (if (= :aggregate (:type col-def))
                              (reduced true)
                              false)))
                        false
                        (:projection rel))]
     (if has-group-by
       (let [{:keys [query params]} (with-group-by-and-having empty-acc env rel [])
             acc' (-> acc
                    (update :query conj (str "GROUP BY " (str/join ", " query)))
                    (update :params into params))]
         (with-having acc' env rel))
       acc)))
  ([acc env rel path-prefix]
   (let [projection (get-in rel [:projection])
         acc'       (reduce
                      (fn [acc alias]
                        (let [col-id    (get-in rel [:aliases->ids alias])
                              col-def   (get-in rel [:columns col-id])
                              path-prefix-names (mapv name path-prefix)
                              col-path  (conj path-prefix-names (name alias))
                              col-name (if (seq path-prefix) (path-prefix-join (rest col-path)) (:name col-def))
                              rel-alias (if (seq path-prefix) (first path-prefix-names) (get-in rel [:spec :name]))
                              col-type (:type col-def)]
                          (cond

                            (or (seq path-prefix) (= col-type :concrete))
                            (update acc :query conj (str (q rel-alias) "." (q col-name)))

                            (and (not (seq path-prefix)) (= :computed col-type))
                            (let [{:keys [query params]} (compile-value-expression empty-acc env rel (:value-expression col-def))]
                              (-> acc
                                (update :params into params)
                                (update :query conj (str (str/join " " query)))))

                            :else acc)))
                      acc
                      (sort projection))]
     (reduce-kv
       (fn [acc' alias {:keys [relation]}]
         (with-group-by-and-having acc' env relation (conj path-prefix alias)))
       acc'
       (get-in rel [:joins])))))

(defn with-order-by [acc env rel]
  (let [order-by (:order-by rel)]
    (if (seq order-by)
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
          (update :query conj (str "ORDER BY " (str/join ", " query)))))
      acc)))

(defn with-limit [acc _ rel]
  (if-let [limit (:limit rel)]
    (update acc :query conj (str "LIMIT " limit))
    acc))

(defn with-offset [acc _ rel]
  (if-let [offset (:offset rel)]
    (update acc :query conj (str "OFFSET " offset))
    acc))

(defn format-query-without-params-resolution [env rel]
  (let [{:keys [query params]} (-> {:query ["SELECT"] :params []}
                                 (with-distinct env rel)
                                 (with-projection env rel)
                                 (with-from env rel)
                                 (with-joins env rel)
                                 (with-where env rel)
                                 (with-group-by-and-having env rel)
                                 (with-order-by env rel)
                                 (with-offset env rel)
                                 (with-limit env rel))]
    (into [(str/join " " query)] params)))

(defn format-query [env rel param-values]
  (let [[query & params] (format-query-without-params-resolution env rel)
        resolved-params (if param-values (map (fn [p] (if (fn? p) (p param-values) p)) params) params)]
    (into [query] resolved-params)))