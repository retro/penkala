(ns com.verybigthings.penkala.statement.select2
  (:require [com.verybigthings.penkala.statement.value-expression :refer [compile-vex]]
            [clojure.string :as str]
            [com.verybigthings.penkala.util.core :refer [q path-prefix-join join-separator]]))

(defn add-from [acc env rel]
  (let [rel-name   (get-in rel [:spec :name])
        rel-schema (get-in rel [:spec :schema])]
    (update acc :query into ["FROM"
                             (q rel-name)
                             "AS"
                             (q rel-name)])))

(defn add-projection
  ([acc env rel]
   (let [{:keys [query params]} (add-projection {:query [] :params []} env rel [])]
     (-> acc
       (update :query conj (str/join ", " query))
       (update :params into params))))
  ([acc env rel path-prefix]
   (let [projection (get-in rel [:projection])
         {:keys [query params]}
         (reduce
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
                 :virtual
                 (let [{:keys [query params]} (compile-vex {:query [] :params []} env rel (:value-expression col-def))]
                   (-> acc
                     (update :params into params)
                     (update :query conj (str (str/join " " query) " AS " (q (path-prefix-join col-alias)))))))))
           {:query [] :params []}
           projection)]
     (reduce-kv
       (fn [acc' alias {:keys [relation]}]
         (add-projection acc' env relation (conj path-prefix alias)))
       (-> acc
         (update :params into params)
         (update :query into query))
       (get-in rel [:joins])))))

(defn add-where [acc env rel]
  (if-let [where (get-in rel [:where])]
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
     (get-in rel [:joins]))))

(defn format-query [env rel param-values]
  (let [{:keys [query params]} (-> {:query ["SELECT"] :params []}
                                 (add-projection env rel)
                                 (add-from env rel)
                                 (add-joins env rel)
                                 (add-where env rel))]
    (into [(str/join " " query)] params)))