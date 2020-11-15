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
            [com.verybigthings.penkala.util.decompose :as d]
            [com.verybigthings.penkala.env :as env])
  (:import [com.github.vertical_blank.sqlformatter SqlFormatter]))

(def default-next-jdbc-options {:builder-fn rs/as-unqualified-lower-maps})
(def get-env-next-jdbc-options {:builder-fn rs/as-unqualified-kebab-maps})

(def internal-db-scripts
  (->> [(h/map-of-db-fns "com/verybigthings/penkala/db_scripts/document-table.sql")
        (h/map-of-db-fns "com/verybigthings/penkala/db_scripts/enums.sql")
        (h/map-of-db-fns "com/verybigthings/penkala/db_scripts/functions.sql")
        (h/map-of-db-fns "com/verybigthings/penkala/db_scripts/sequences.sql")
        (h/map-of-db-fns "com/verybigthings/penkala/db_scripts/tables.sql")
        (h/map-of-db-fns "com/verybigthings/penkala/db_scripts/views.sql")]
    (apply merge)))

(def hugsql-adapter (next-adapter/hugsql-adapter-next-jdbc get-env-next-jdbc-options))

(defn exec-internal-db-script
  ([db-spec db-script-name] (exec-internal-db-script db-spec db-script-name {}))
  ([db-spec db-script-name params]
   (let [db-script-fn (get-in internal-db-scripts [db-script-name :fn])]
     (db-script-fn db-spec params {:quoting :ansi :adapter hugsql-adapter}))))

(defn with-relations [env relations]
  (reduce
    (fn [acc rel-spec]
      (let [rel        (r/spec->relation rel-spec)
            rel-schema (get-in rel [:spec :schema])
            rel-name   (-> rel (get-in [:spec :name]) ->kebab-case)
            rel-ns     (when-not (= (::env/schema env) rel-schema) (->kebab-case rel-schema))
            rel-key    (if rel-ns (keyword rel-ns rel-name) (keyword rel-name))]
        (assoc acc rel-key rel)))
    env
    relations))

(defn get-env
  ([db-spec] (get-env db-spec {}))
  ([db-spec config]
   (let [current-schema (-> (jdbc/execute-one! db-spec ["SELECT current_schema"] get-env-next-jdbc-options) :current-schema)
         server-version (-> (jdbc/execute-one! db-spec ["SHOW server_version"] get-env-next-jdbc-options) :server-version)]
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
      (-> {}
         (env/with-current-schema current-schema)
         (env/with-db db-spec)
         (with-relations (concat tables views)))))))

(defn prettify-sql [sql]
  (SqlFormatter/format sql))

(defn select!
  ([env relation] (select! env relation {} {}))
  ([env relation params] (select! env relation params {}))
  ([env relation params decomposition-schema-overrides]
   (let [db                   (::env/db env)
         relation'            (if (keyword? relation) (get env relation) relation)
         sqlvec               (r/get-select-query relation' env params)
         decomposition-schema (d/infer-schema relation' decomposition-schema-overrides)]
     (println (prettify-sql (first sqlvec)))
     (println (rest sqlvec))
     (println (first sqlvec))
     (->> (jdbc/execute! db sqlvec default-next-jdbc-options)
       (d/decompose decomposition-schema)))))

(defn select-one!
  ([env relation] (select-one! env relation {} {}))
  ([env relation params] (select-one! env relation params {}))
  ([env relation params decomposition-schema-overrides]
   (let [res (select! env relation params decomposition-schema-overrides)]
     (if (coll? res)
       (first res)
       res))))