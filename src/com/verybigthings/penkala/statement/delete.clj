(ns com.verybigthings.penkala.statement.delete
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

(defn with-returning [acc env deletable]
  ;; deletable might have a from table set which will be reusing the joins map
  ;; and we don't want the infer function to pick it up, so we remove it here
  (if (:projection deletable)
    (-> acc
      (update :query conj "RETURNING")
      (sel/with-projection env (dissoc deletable :joins)))
    acc))

(defn with-using [acc env deletable]
  (if-let [using (:joins deletable)]
    (reduce-kv
      (fn [acc' alias f]
        (let [using-relation (:relation f)
              [using-query & using-params] (binding [sel/*scopes* (conj sel/*scopes* {:env env :rel deletable})]
                                           (sel/format-query-without-params-resolution env using-relation))
              using-clause ["USING" (str "(" using-query ")") (q (get-rel-alias-with-prefix env alias))]]
          (-> acc'
            (update :query into using-clause)
            (update :params into using-params))))
      acc
      using)
    acc))

(defn format-query [env deletable param-values]
  (let [{:keys [query params]} (-> {:query [(if (:only deletable) "DELETE FROM ONLY" "DELETE FROM")
                                            (get-schema-qualified-relation-name env deletable)]
                                    :params []}
                                 (with-using env deletable)
                                 (sel/with-where env deletable)
                                 (with-returning env deletable))
        resolved-params (if param-values (map (fn [p] (if (fn? p) (p param-values) p)) params) params)]
    (into [(str/join " " query)] resolved-params)))