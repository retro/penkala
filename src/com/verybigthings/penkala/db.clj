(ns com.verybigthings.penkala.db
  (:require [hugsql.adapter.next-jdbc :as next-adapter]
            [hugsql.core :as h]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [clojure.string :as str]
            [camel-snake-kebab.core :refer [->kebab-case]]
            [com.verybigthings.penkala.next-jdbc]
            [com.verybigthings.penkala.relation :as r]
            [com.verybigthings.penkala.util.core :refer [select-keys-with-default]]
            [com.verybigthings.penkala.statement.select :as select])
  (:import [com.github.vertical_blank.sqlformatter SqlFormatter]))

(def default-next-jdbc-options {:builder-fn rs/as-unqualified-lower-maps})

(def internal-db-scripts
  (->> [(h/map-of-db-fns "com/verybigthings/penkala/db_scripts/document-table.sql")
        (h/map-of-db-fns "com/verybigthings/penkala/db_scripts/enums.sql")
        (h/map-of-db-fns "com/verybigthings/penkala/db_scripts/functions.sql")
        (h/map-of-db-fns "com/verybigthings/penkala/db_scripts/sequences.sql")
        (h/map-of-db-fns "com/verybigthings/penkala/db_scripts/tables.sql")
        (h/map-of-db-fns "com/verybigthings/penkala/db_scripts/views.sql")]
    (apply merge)))


(def hugsql-adapter (next-adapter/hugsql-adapter-next-jdbc))

(defn exec-internal-db-script
  ([db-spec db-script-name] (exec-internal-db-script db-spec db-script-name {}))
  ([db-spec db-script-name params]
   (let [db-script-fn (get-in internal-db-scripts [db-script-name :fn])]
     (db-script-fn db-spec params {:quoting :ansi :adapter hugsql-adapter}))))

(defn register-relations [db relations]
  (reduce
    (fn [db' r-spec]
      (let [r        (r/make-relation db' r-spec)
            r-schema (:db/schema r)
            k-ns     (if (= (:db/schema db) r-schema) "relation" (str r-schema ".relation"))
            k        (keyword k-ns (-> r :relation/name ->kebab-case))]
        (assoc db' k r)))
    db
    relations))

(defn assert-server-version [server-version]
  (let [major (-> server-version (str/split #"\.") first Integer/parseInt)]
    (assert (>= major 12) "com.verybigthings/penkala requires PostgreSQL version 12 or bigger.")))

(defn inspect
  ([db-spec] (inspect db-spec {}))
  ([db-spec config]
   (let [current-schema (-> (jdbc/execute-one! db-spec ["SELECT current_schema"] default-next-jdbc-options) :current_schema)
         server-version (-> (jdbc/execute-one! db-spec ["SHOW server_version"] default-next-jdbc-options) :server_version)
         db {:db/schema current-schema
             :db/spec db-spec}]
     (assert-server-version server-version)
     (let [enums     (exec-internal-db-script db-spec :get-enums)
           functions (exec-internal-db-script db-spec :get-functions
                       (select-keys-with-default
                         config [:functions/forbidden :functions/allowed :functions/exceptions :schemas/allowed] nil))
           sequences (exec-internal-db-script db-spec :get-sequences)
           tables    (exec-internal-db-script db-spec :get-tables
                       (select-keys-with-default
                         config [:relations/forbidden :relations/allowed :relations/exceptions :schemas/allowed] nil))
           views     (exec-internal-db-script db-spec :get-views
                       (select-keys-with-default
                         config [:relations/forbidden :relations/allowed :relations/exceptions :schemas/allowed] nil))]
       (clojure.pprint/pprint tables)
       (-> db
         (register-relations (concat tables views)))))))

(defn prettify-sql [sql]
  (SqlFormatter/format sql))

(defn query [db-spec db relation]
  (let [relation' (if (keyword? relation) (get db relation) relation)
        sqlvec (select/get-query db relation')]
    (println (prettify-sql (first sqlvec)))
    (println (rest sqlvec))
    (->> (jdbc/execute! db-spec sqlvec default-next-jdbc-options)
      (r/decompose-results relation'))))

(defn query-one [db-spec db relation]
  (let [relation' (if (keyword? relation) (get db relation) relation)
        sqlvec (select/get-query db (-> relation' (r/limit 1)))]
    (println (prettify-sql (first sqlvec)))
    (println (rest sqlvec))
    (->> (jdbc/execute! db-spec sqlvec default-next-jdbc-options)
      (r/decompose-results relation')
      first)))