(ns com.verybigthings.penkala.statement.insert
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

(defn with-returning [acc env insertable]
  (if (:projection insertable)
    (-> acc
        (update :query conj "RETURNING")
        (sel/with-projection env (dissoc insertable :joins)))
    acc))

(defn with-on-conflict-where [acc env insertable]
  (if-let [where (get-in insertable [:on-conflict :where])]
    (let [{:keys [query params]}
          (sel/compile-value-expression sel/empty-acc env insertable where)]
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
                   {:keys [query params]} (sel/compile-value-expression sel/empty-acc env insertable col-update)]
               (-> acc'
                   (update :query conj (str/join " " (into [(q col-name) "="] query)))
                   (update :params into params))))
           sel/empty-acc
           updates)]
      (-> acc
          (update :query into ["SET" (str/join ", " query)])
          (update :params into params)))
    acc))

(defn with-on-conflict-conflict-target [acc env insertable]
  (if-let [[conflict-target-type conflict-target] (get-in insertable [:on-conflict :conflict-target])]
    (do
      (case conflict-target-type
        :value-expressions
        (let [{:keys [query params]}
              (reduce
               (fn [acc conflict]
                 (let [{:keys [query params]} (sel/compile-value-expression sel/empty-acc env insertable conflict)]
                   (-> acc
                       (update :query conj (str/join " " query))
                       (update :params into params))))
               sel/empty-acc
               conflict-target)]
          (-> acc
              (update :query conj (str "(" (str/join ", " query) ")"))
              (update :params into params)))
        :on-constraint
        (update acc :query into ["ON CONSTRAINT" conflict-target])))
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

(defn with-col-value [acc env insertable col-value]
  (let [{:keys [query params]} (sel/compile-value-expression sel/empty-acc env insertable col-value)]
    (-> acc
        (update :params into params)
        (update :query conj (str "(" (str/join " " query) ")")))))

(defn with-values [acc env insertable insertable-columns data]
  (let [{:keys [query params]}
        (reduce
         (fn [acc' entry]
           (let [{:keys [query params]}
                 (reduce
                  (fn [entry-acc col]
                    (let [col-value (get entry col)]
                      (if col-value
                        (with-col-value entry-acc env insertable col-value)
                        (update entry-acc :query conj "DEFAULT"))))
                  {:query [] :params []}
                  insertable-columns)]
             (-> acc'
                 (update :query conj (str "(" (str/join ", " query) ")"))
                 (update :params into params))))
         {:query [] :params []}
         data)]
    (-> acc
        (update :query conj (str/join ", " query))
        (update :params into params))))

(defn with-columns-and-values [acc env insertable]
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
        (update :query conj (str "(" (str/join ", " insertable-columns-names) ")"))
        (update :query conj "VALUES")
        (with-values env insertable insertable-columns data'))))

(defn format-query [env insertable]
  (let [{:keys [query params]} (-> {:query ["INSERT INTO"
                                            (get-schema-qualified-relation-name env insertable)]
                                    :params []}
                                   (with-columns-and-values env insertable)
                                   (with-on-conflict env insertable)
                                   (with-returning env insertable))]
    (into [(str/join " " query)] params)))