(ns com.verybigthings.penkala.statement.update
  (:require [clojure.string :as str]
            [com.verybigthings.penkala.util :refer [q vec-conj]]
            [com.verybigthings.penkala.statement.shared
             :refer [get-rel-alias-with-prefix
                     get-rel-alias
                     get-rel-schema
                     get-schema-qualified-relation-name
                     make-rel-alias-prefix]]
            [com.verybigthings.penkala.statement.select :as sel]
            [camel-snake-kebab.core :refer [->SCREAMING_SNAKE_CASE_STRING ->snake_case_string]]
            [com.verybigthings.penkala.env :as env]
            [clojure.set :as set]))

(defn with-returning [acc env updatable]
  ;; Updatable might have a from table set which will be reusing the joins map
  ;; and we don't want the infer function to pick it up, so we remove it here
  (if (:projection updatable)
    (-> acc
      (update :query conj "RETURNING")
      (sel/with-projection env (dissoc updatable :joins)))
    acc))

(defn with-from [acc env updatable]
  (if-let [from (:joins updatable)]
    (let [{:keys [query params]}
          (reduce-kv
            (fn [acc' alias f]
              (let [from-relation (:relation f)
                    [from-query & from-params] (binding [sel/*scopes* (conj sel/*scopes* {:env env :rel updatable})]
                                                 (sel/format-query-without-params-resolution env from-relation))
                    from-clause (str "(" from-query ") " (q (get-rel-alias-with-prefix env alias)))]
                (-> acc'
                  (update :query conj from-clause)
                  (update :params into from-params))))
            sel/empty-acc
            from)]
      (-> acc
        (update :query conj "FROM")
        (update :query conj (str/join ", " query))
        (update :params into params)))
    acc))

(defn with-updates [acc env updatable]
  (reduce-kv
    (fn [acc' col col-update]
      (let [col-id   (get-in updatable [:aliases->ids col])
            col-name (get-in updatable [:columns col-id :name])
            {:keys [query params]} (sel/compile-value-expression sel/empty-acc env updatable col-update)]
        (-> acc'
          (update :query into ["SET" (q col-name) "="])
          (update :query into query)
          (update :params into params))))
    acc
    (:updates updatable)))

(defn format-query [env updatable param-values]
  (let [{:keys [query params]} (-> {:query [(if (:only updatable) "UPDATE ONLY" "UPDATE")
                                            (get-schema-qualified-relation-name env updatable)]
                                    :params []}
                                 (with-updates env updatable)
                                 (with-from env updatable)
                                 (sel/with-where env updatable)
                                 (with-returning env updatable))
        resolved-params (if param-values (map (fn [p] (if (fn? p) (p param-values) p)) params) params)]
    (into [(str/join " " query)] resolved-params)))