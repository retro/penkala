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
      (sel/with-projection env insertable))
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

(defn with-values [acc env insertable-columns data]
  (let [{:keys [query params]}
        (reduce
          (fn [acc' entry]
            (let [{:keys [query params]}
                  (reduce
                    (fn [entry-acc col]
                      (let [col-value (get entry col)]
                        (if col-value
                          (-> entry-acc
                            (update :query conj "?")
                            (update :params conj col-value))
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

(defn with-columns-and-values [acc env insertable data]
  (let [data'              (if (map? data) [data] data)
        insertable-columns (get-insertable-columns insertable data')
        insertable-columns-names (map
                                   (fn [c]
                                     (let [col-id   (get-in insertable [:aliases->ids c])
                                           col-name (get-in insertable [:columns col-id :name])]
                                       (q col-name)))
                                   insertable-columns)]
    (-> acc
      (update :query conj (str "(" (str/join ", " insertable-columns-names) ")"))
      (update :query conj "VALUES")
      (with-values env insertable-columns data'))))

(defn format-query [env insertable data]
  (let [{:keys [query params]} (-> {:query ["INSERT INTO"
                                            (get-schema-qualified-relation-name env insertable)]
                                    :params []}
                                 (with-columns-and-values env insertable data)
                                 (with-returning env insertable))]
    (into [(str/join " " query)] params)))