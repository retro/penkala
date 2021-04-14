(ns com.verybigthings.penkala.next-jdbc
  (:require [camel-snake-kebab.core :refer [->kebab-case]]
            [next.jdbc.result-set :as rs]
            [jsonista.core :as j]
            [hugsql.adapter.next-jdbc :as next-adapter]
            [hugsql.core :as h]
            [next.jdbc :as jdbc]
            [com.verybigthings.penkala.relation :as r]
            [com.verybigthings.penkala.util :refer [select-keys-with-default]]
            [com.verybigthings.penkala.decomposition :as d]
            [com.verybigthings.penkala.env :as env])
  (:import (org.postgresql.util PGobject)
           (com.github.vertical_blank.sqlformatter SqlFormatter)))

(defn pgobj->clj [^org.postgresql.util.PGobject pgobj]
  (let [type  (.getType pgobj)
        value (.getValue pgobj)]
    (case type
      "json" (j/read-value value j/keyword-keys-object-mapper)
      "jsonb" (j/read-value value j/keyword-keys-object-mapper)
      "citext" (str value)
      value)))

(extend-protocol next.jdbc.result-set/ReadableColumn
  java.sql.Timestamp
  (read-column-by-label [^java.sql.Timestamp v _]
    (.toLocalDateTime v))
  (read-column-by-index [^java.sql.Timestamp v _2 _3]
    (.toLocalDateTime v))
  java.sql.Date
  (read-column-by-label [^java.sql.Date v _]
    (.toLocalDate v))
  (read-column-by-index [^java.sql.Date v _2 _3]
    (.toLocalDate v))
  java.sql.Time
  (read-column-by-label [^java.sql.Time v _]
    (.toLocalTime v))
  (read-column-by-index [^java.sql.Time v _2 _3]
    (.toLocalTime v))
  java.sql.Array
  (read-column-by-label [^java.sql.Array v _]
    (vec (.getArray v)))
  (read-column-by-index [^java.sql.Array v _2 _3]
    (vec (.getArray v)))
  org.postgresql.util.PGobject
  (read-column-by-label [^org.postgresql.util.PGobject pgobj _]
    (pgobj->clj pgobj))
  (read-column-by-index [^org.postgresql.util.PGobject pgobj _2 _3]
    (pgobj->clj pgobj)))

(defn clj->jsonb-pgobj [value]
  (doto (PGobject.)
    (.setType "jsonb")
    (.setValue (j/write-value-as-string value))))

(extend-protocol next.jdbc.prepare/SettableParameter
  clojure.lang.IPersistentMap
  (set-parameter [^clojure.lang.IPersistentMap v ^java.sql.PreparedStatement stmt ^long idx]
    (.setObject stmt idx (clj->jsonb-pgobj v)))
  clojure.lang.IPersistentVector
  (set-parameter [^clojure.lang.IPersistentVector v ^java.sql.PreparedStatement stmt ^long idx]
    (let [conn      (.getConnection stmt)
          meta      (.getParameterMetaData stmt)
          type-name (.getParameterTypeName meta idx)]
      (if-let [elem-type (when (= (first type-name) \_)
                           (apply str (rest type-name)))]
        (.setObject stmt idx (.createArrayOf conn elem-type (to-array v)))
        (.setObject stmt idx (clj->jsonb-pgobj v))))))


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
  "Gets the env information from a database. It will list all tables and views and return a map where keys are table
  names and values are relations. In case of the non-default schemas, namespaced keys will be used, where the namespace
  will be the schema name."
  ([db-spec] (get-env db-spec {}))
  ([db-spec config]
   (let [current-schema (-> (jdbc/execute-one! db-spec ["SELECT current_schema"] get-env-next-jdbc-options) :current-schema)
         #_#_enums (exec-internal-db-script db-spec :get-enums)
         #_#_functions (exec-internal-db-script db-spec :get-functions
                                                (select-keys-with-default
                                                 config [:functions/forbidden :functions/allowed :functions/exceptions :schemas/allowed] nil))
         #_#_sequences (exec-internal-db-script db-spec :get-sequences)
         tables (exec-internal-db-script db-spec :get-tables
                                         (select-keys-with-default
                                          config [:relations/forbidden :relations/allowed :relations/exceptions :schemas/allowed] nil))
         views  (exec-internal-db-script db-spec :get-views
                                         (select-keys-with-default
                                          config [:relations/forbidden :relations/allowed :relations/exceptions :schemas/allowed] nil))]
     (-> {}
         (env/with-current-schema current-schema)
         (env/with-db db-spec)
         (with-relations (concat tables views))))))

(defn prettify-sql [sql]
  (SqlFormatter/format sql))

(defn validate-relation [env rel]
  (let [rel' (get env rel)]
    (when (nil? rel')
      (throw (ex-info "Relation does not exist" {:relation rel})))
    rel'))

(defn select!
  "Selects the results based on the relation and returns them decomposed."
  ([env relation] (select! env relation {} {}))
  ([env relation params] (select! env relation params {}))
  ([env relation params decomposition-schema-overrides]
   (let [db                   (::env/db env)
         relation'            (if (keyword? relation) (validate-relation env relation) relation)
         sqlvec               (r/get-select-query relation' env params)
         decomposition-schema (d/infer-schema relation' decomposition-schema-overrides)]
     (->> (jdbc/execute! db sqlvec default-next-jdbc-options)
          (d/decompose decomposition-schema)))))

(defn select-one!
  "Selects the results based on the relation and returns the first one decomposed. This will not change the relation by
  adding a limit"
  ([env relation] (select-one! env relation {} {}))
  ([env relation params] (select-one! env relation params {}))
  ([env relation params decomposition-schema-overrides]
   (let [res (select! env relation params decomposition-schema-overrides)]
     (if (coll? res)
       (first res)
       res))))

(defn insert!
  ([env insertable inserts] (insert! env insertable inserts {}))
  ([env insertable inserts decomposition-schema-overrides]
   (let [db          (::env/db env)
         insertable' (-> (if (keyword? insertable)
                           (-> env (validate-relation insertable) (r/->insertable))
                           insertable)
                         (r/with-inserts inserts))
         sqlvec      (r/get-insert-query insertable' env)]
     (if (:projection insertable')
       (let [;; If we're using insertable with on-conflict-do-update, an implicit join to the "excluded"
             ;; table is created. This will remove it so it's not picked up by the decomposition schema
             ;; inference.
             decomposition-schema (d/infer-schema (dissoc insertable' :joins) decomposition-schema-overrides)
             res                  (->> (jdbc/execute! db sqlvec default-next-jdbc-options)
                                       (d/decompose decomposition-schema))]
         (if (map? inserts) (first res) res))
       (jdbc/execute-one! db sqlvec default-next-jdbc-options)))))

(defn update!
  ([env updatable updates] (update! env updatable updates {} {}))
  ([env updatable updates params] (update! env updatable updates params {}))
  ([env updatable updates params decomposition-schema-overrides]
   (let [db         (::env/db env)
         updatable' (-> (if (keyword? updatable)
                          (-> env (validate-relation updatable) (r/->updatable))
                          updatable)
                        (r/with-updates updates))
         sqlvec     (r/get-update-query updatable' env params)]
     (if (:projection updatable')
       (let [;; Updatable might have a from table set which will be reusing the joins map
             ;; and we don't want the infer function to pick it up, so we remove it here
             decomposition-schema (d/infer-schema (dissoc updatable' :joins) decomposition-schema-overrides)
             res                  (->> (jdbc/execute! db sqlvec default-next-jdbc-options)
                                       (d/decompose decomposition-schema))]
         res)
       (jdbc/execute-one! db sqlvec default-next-jdbc-options)))))


(defn delete!
  ([env deletable] (delete! env deletable {} {}))
  ([env deletable params] (delete! env deletable params {}))
  ([env deletable params decomposition-schema-overrides]
   (let [db         (::env/db env)
         deletable' (if (keyword? deletable)
                      (-> env (validate-relation deletable) (r/->deletable))
                      deletable)
         sqlvec     (r/get-delete-query deletable' env params)]
     (if (:projection deletable')
       (let [;; Deletable might have an using table set which will be reusing the joins map
             ;; and we don't want the infer function to pick it up, so we remove it here
             decomposition-schema (d/infer-schema (dissoc deletable' :joins) decomposition-schema-overrides)
             res                  (->> (jdbc/execute! db sqlvec default-next-jdbc-options)
                                       (d/decompose decomposition-schema))]
         res)
       (jdbc/execute-one! db sqlvec default-next-jdbc-options)))))

(comment
  (def users-spec {:name "users"
                   :columns ["id" "username" "is_admin"]
                   :pk ["id"]
                   :schema "public"})
  (def users-rel (r/spec->relation users-spec))
  (r/get-select-query users-rel {})

  (def admins-rel (r/where users-rel [:is-true :is-admin]))
  (r/get-select-query admins-rel {})

  (def posts-spec {:name "posts"
                   :columns ["id" "user_id" "body"]
                   :pk ["id"]
                   :schema "public"})

  (def posts-rel (r/spec->relation posts-spec))

  (r/get-select-query (r/where posts-rel [:in :user-id (r/select admins-rel [:id])]) {})

  (r/get-select-query (r/join posts-rel :left admins-rel :author [:= :user-id :author/id]) {})

  (require '[com.verybigthings.penkala.helpers :refer [param]])

  (r/get-select-query (r/where posts-rel [:= :user-id (param :user/id)]) {} {:user/id 1}))