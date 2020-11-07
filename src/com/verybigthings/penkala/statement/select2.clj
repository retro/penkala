(ns com.verybigthings.penkala.statement.select2
  (:require [com.verybigthings.penkala.statement.value-expression :refer [compile-vex]]
            [clojure.string :as str]
            [com.verybigthings.penkala.util.core :refer [q path-prefix-join join-separator]]))

(defn get-rel-alias [rel]
  (q (or (get-in rel [:spec :name]) (get-in rel [:state :alias]))))

(defn get-col-prefix [rel]
  (when-let [alias (get-in rel [:state :alias])]
    (str alias join-separator)))

(defn add-from [acc env rel]
  (let [rel-name   (get-in rel [:spec :name])
        rel-schema (get-in rel [:spec :schema])
        rel-alias  (get-in rel [:state :alias])]
    (update acc :query into ["FROM"
                             (str (q rel-schema) "." (q rel-name))
                             "AS"
                             (q (str (get-col-prefix rel) (or rel-alias rel-name)))])))

(defn add-projection
  ([acc env rel]
   (let [{:keys [query params]} (add-projection {:query [] :params []} env rel [])]
     (-> acc
       (update :query conj (str/join ", " query))
       (update :params into params))))
  ([acc env rel path-prefix]
   (let [projection (get-in rel [:state :projection])
         query      (reduce
                      (fn [acc alias]
                        (let [col-id    (get-in rel [:state :column-aliases alias])
                              col       (get-in rel [:state :columns col-id])
                              rel-alias (if (seq path-prefix)
                                          (path-prefix-join (map name path-prefix))
                                          (get-in rel [:spec :name]))
                              col-alias (if (seq path-prefix) [rel-alias (name alias)] [(name alias)])]
                          (conj acc (str (q rel-alias) "." (q col) " AS " (q (path-prefix-join col-alias))))))
                      []
                      projection)]
     (reduce-kv
       (fn [acc' alias {:keys [relation]}]
         (add-projection acc' env relation (conj path-prefix alias)))
       (update acc :query into query)
       (get-in rel [:state :joins])))))

(defn add-where [acc env rel]
  (if-let [where (get-in rel [:state :where])]
    (-> acc
      (update :query conj "WHERE")
      (compile-vex env rel where))
    acc))

(defn add-joins
  ([acc env rel] (add-joins acc env rel []))
  ([acc env rel path-prefix]
   (reduce-kv
     (fn [acc' alias j]
       (let [join-sql-type ({:left "LEFT OUTER" :inner "INNER"} (:type j))
             join-alias (->> (conj path-prefix alias) (map name) path-prefix-join)
             join-relation (:relation j)
             join-clause [join-sql-type "JOIN" (q (get-in join-relation [:spec :name])) (q join-alias) "ON"]
             {:keys [query params]} (compile-vex {:query join-clause :params []} (assoc env :join/path-prefix path-prefix) rel (:on j))]
         (-> acc'
           (update :params into params)
           (update :query into query)
           (add-joins env join-relation (conj path-prefix alias)))))
     acc
     (get-in rel [:state :joins]))))

(defn format-query [env rel param-values]
  (let [{:keys [query params]} (-> {:query ["SELECT"] :params []}
                                 (add-projection env rel)
                                 (add-from env rel)
                                 (add-joins env rel)
                                 (add-where env rel))]
    (into [(str/join " " query)] params)))