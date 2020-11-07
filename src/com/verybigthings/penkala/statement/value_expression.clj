(ns com.verybigthings.penkala.statement.value-expression
  (:require [camel-snake-kebab.core :refer [->SCREAMING_SNAKE_CASE_STRING]]
            [clojure.string :as str]
            [com.verybigthings.penkala.util.core :refer [q expand-join-path path-prefix-join join-separator]]))

(defn get-resolved-column-prefix [env rel col]
  (let [col-path (concat (:join/path-prefix env) (:path col))
        rel-name (get-in rel [:spec :name])]
    (if (seq col-path)
      (q (path-prefix-join (map name col-path)))
      (q rel-name))))

(defmulti compile-function-call (fn [acc env rel function-name args] function-name))
(defmulti compile-vex (fn [acc env rel [vex-type & _]] vex-type))

(defmethod compile-function-call :default [acc env rel function-name args]
  (let [sql-function-name (->SCREAMING_SNAKE_CASE_STRING function-name)
        {:keys [query params]} (reduce
                                 (fn [acc arg]
                                   (compile-vex acc env rel arg))
                                 {:query [] :params []}
                                 args)]
    (-> acc
      (update :query conj (str sql-function-name "(" (str/join " " query) ")"))
      (update :params into params))))


(defmethod compile-vex :default [acc env rel [vex-type & args]]
  (throw
    (ex-info
      (str "com.verybigthins.penkala.statement.value-expression/compile-vex multimethod not implemented for " vex-type)
      {:type vex-type
       :args args})))

(defmethod compile-vex :function-call [acc env rel [_ {:keys [fn args]}]]
  (compile-function-call acc env rel fn args))

(defmethod compile-vex :resolved-column [acc env rel [_ col]]
  (let [col-path (:path col)
        col-rel (if (seq col-path) (get-in rel (expand-join-path col-path)) rel)
        col-def (get-in col-rel [:columns (:id col)])]
    (case (:type col-def)
      :concrete
      (update acc :query conj (str (get-resolved-column-prefix env rel col) "." (q (:name col-def))))
      :virtual
      (compile-vex acc env rel (:value-expression col-def)))))

(defmethod compile-vex :value [acc _ _ [_ val]]
  (-> acc
    (update :query conj "?")
    (update :params conj val)))

(defmethod compile-vex :binary-operation [{:keys [query params]} env rel [_ {:keys [op arg1 arg2]}]]
  (let [sql-op (-> op ->SCREAMING_SNAKE_CASE_STRING (str/replace #"_" " "))
        arg1-acc (compile-vex {:query [] :params []} env rel arg1)
        arg2-acc (compile-vex  {:query [] :params []} env rel arg2)]
    {:query (-> query (into (:query arg1-acc)) (conj sql-op) (into (:query arg2-acc)))
     :params (-> params (into (:params arg1-acc)) (into (:params arg2-acc)))}))

(defmethod compile-vex :boolean [acc _ _ [_ value]]
  (update acc :query conj (if value "TRUE" "FALSE")))

(defmethod compile-vex :connective [acc env rel [_ {:keys [op args]}]]
  (if (= 1 (count args))
    (compile-vex acc env rel (first args))
    (let [sql-op (->SCREAMING_SNAKE_CASE_STRING op)
          {:keys [query params]} (reduce
                                   (fn [acc arg]
                                     (-> acc
                                       (compile-vex env rel arg)
                                       (update :query conj sql-op)))
                                   {:query [] :params []}
                                   args)]
      (-> acc
        (update :params into params)
        (update :query conj (str "(" (str/join " " (butlast query)) ")"))))))