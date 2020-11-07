(ns com.verybigthings.penkala.statement.value-expression
  (:require [camel-snake-kebab.core :refer [->SCREAMING_SNAKE_CASE_STRING]]
            [clojure.string :as str]
            [com.verybigthings.penkala.util.core :refer [q expand-join-path path-prefix-join join-separator]]))

(defn get-resolved-column-prefix [env rel col]
  (println "RESOLVED COLUMN" col)
  (let [col-path (concat (:join/path-prefix env) (:path col))
        rel-name (get-in rel [:spec :name])]
    (if (seq col-path)
      (q (path-prefix-join (map name col-path)))
      (q rel-name))))

(defmulti compile-vex (fn [acc env rel [vex-type & _]] vex-type))

(defmethod compile-vex :default [acc env rel [vex-type & args]]
  (throw
    (ex-info
      (str "com.verybigthins.penkala.statement.value-expression/compile-vex multimethod not implemented for " vex-type)
      {:value-expression/type vex-type
       :value-expression/args args})))

(defmethod compile-vex :resolved-column [acc env rel [_ col]]
  (let [col-path (:path col)
        col-rel (if (seq col-path) (get-in rel (expand-join-path col-path)) rel)
        col-def (get-in col-rel [:state :columns (:id col)])]
    (update acc :query conj (str (get-resolved-column-prefix env rel col) "." (q col-def)))))

(defmethod compile-vex :value [acc env rel [_ val]]
  (-> acc
    (update :query conj "?")
    (update :params conj val)))

(defmethod compile-vex :binary-operation [{:keys [query params]} env rel [_ {:keys [op arg1 arg2]}]]
  (let [sql-op (-> op ->SCREAMING_SNAKE_CASE_STRING (str/replace #"_" " "))
        arg1-acc (compile-vex {:query [] :params []} env rel arg1)
        arg2-acc (compile-vex  {:query [] :params []} env rel arg2)]
    {:query (-> query (into (:query arg1-acc)) (conj sql-op) (into (:query arg2-acc)))
     :params (-> params (into (:params arg1-acc)) (into (:params arg2-acc)))}))