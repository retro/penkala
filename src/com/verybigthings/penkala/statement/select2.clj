(ns com.verybigthings.penkala.statement.select2
  (:require [com.verybigthings.penkala.statement.value-expression :refer [compile-value-expression]]
            [clojure.string :as str]
            [com.verybigthings.penkala.util.core :refer [q path-prefix-join join-separator]]
            [camel-snake-kebab.core :refer [->SCREAMING_SNAKE_CASE_STRING]]))

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
                        (let [col-id    (get-in rel [:column-aliases alias])
                              col-def   (get-in rel [:columns col-id])
                              rel-alias (if (seq path-prefix)
                                          (path-prefix-join (map name path-prefix))
                                          (get-in rel [:spec :name]))
                              col-alias (if (seq path-prefix) [rel-alias (name alias)] [(name alias)])]
                          (case (:type col-def)
                            :concrete
                            (update acc :query conj (str (q rel-alias) "." (q (:name col-def)) " AS " (q (path-prefix-join col-alias))))
                            (:virtual :aggregate)
                            (let [{:keys [query params]} (compile-value-expression empty-acc env rel (:value-expression col-def))]
                              (-> acc
                                (update :params into params)
                                (update :query conj (str (str/join " " query) " AS " (q (path-prefix-join col-alias)))))))))
                      acc
                      projection)]
     (reduce-kv
       (fn [acc' alias {:keys [relation]}]
         (with-projection acc' env relation (conj path-prefix alias)))
       acc'
       (get-in rel [:joins])))))

(defn with-from [acc env rel]
  (let [rel-name (get-in rel [:spec :name])]
    (update acc :query into [(if (:only rel) "FROM ONLY" "FROM")
                             (schema-qualified-relation-name env rel)
                             "AS"
                             (q rel-name)])))

(defn with-joins
  ([acc env rel] (with-joins acc env rel []))
  ([acc env rel path-prefix]
   (reduce-kv
     (fn [acc' alias j]
       (let [join-sql-type ({:left "LEFT OUTER" :inner "INNER"} (:type j))
             join-alias    (->> (conj path-prefix alias) (map name) path-prefix-join)
             join-relation (:relation j)
             join-clause   [join-sql-type "JOIN" (schema-qualified-relation-name env join-relation) (q join-alias) "ON"]
             {:keys [query params]} (compile-value-expression {:query join-clause :params []} (assoc env :join/path-prefix path-prefix) rel (:on j))]
         (-> acc'
           (update :params into params)
           (update :query into query)
           (with-joins env join-relation (conj path-prefix alias)))))
     acc
     (get-in rel [:joins]))))

(defn with-where [acc env rel]
  (if-let [where (:where rel)]
    (-> acc
      (update :query conj "WHERE")
      (compile-value-expression env rel where))
    acc))

(defn with-group-by
  ([acc env rel]
   (let [has-group-by (reduce
                        (fn [_ alias]
                          (let [col-id  (get-in rel [:column-aliases alias])
                                col-def (get-in rel [:columns col-id])]
                            (if (= :aggregate (:type col-def))
                              (reduced true)
                              false)))
                        false
                        (:projection rel))]
     (if has-group-by
       (let [{:keys [query params]} (with-group-by empty-acc env rel [])]
         (-> acc
           (update :query conj (str "GROUP BY " (str/join ", " query)))
           (update :params into params)))
       acc)))
  ([acc env rel path-prefix]
   (let [projection (get-in rel [:projection])
         acc'       (reduce
                      (fn [acc alias]
                        (let [col-id    (get-in rel [:column-aliases alias])
                              col-def   (get-in rel [:columns col-id])
                              rel-alias (if (seq path-prefix)
                                          (path-prefix-join (map name path-prefix))
                                          (get-in rel [:spec :name]))]
                          (case (:type col-def)
                            :concrete
                            (update acc :query conj (str (q rel-alias) "." (q (:name col-def))))
                            :virtual
                            (let [{:keys [query params]} (compile-value-expression empty-acc env rel (:value-expression col-def))]
                              (-> acc
                                (update :params into params)
                                (update :query conj (str (str/join " " query)))))
                            acc)))
                      acc
                      projection)]
     (reduce-kv
       (fn [acc' alias {:keys [relation]}]
         (with-group-by acc' env relation (conj path-prefix alias)))
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

(defn format-query [env rel param-values]
  (let [{:keys [query params]} (-> {:query ["SELECT"] :params []}
                                 (with-distinct env rel)
                                 (with-projection env rel)
                                 (with-from env rel)
                                 (with-joins env rel)
                                 (with-where env rel)
                                 (with-group-by env rel)
                                 (with-order-by env rel)
                                 (with-offset env rel)
                                 (with-limit env rel))
        resolved-params (map (fn [p] (if (fn? p) (p param-values) p)) params)]
    (into [(str/join " " query)] resolved-params)))