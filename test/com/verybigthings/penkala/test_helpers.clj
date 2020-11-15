(ns com.verybigthings.penkala.test-helpers
  (:require [clojure.test :refer :all]
            [next.jdbc :as jdbc]
            [clojure.java.io :as io]
            [next.jdbc.result-set :as rs]
            [com.verybigthings.penkala.db :as db]))

(def ^:dynamic *env* {})

(def db-uri "jdbc:postgresql://localhost:5432/penkala_dev?user=postgres")

(defn reset-db [db-preset]
  (let [sql     (-> (str "com/verybigthings/penkala/test_db_scripts/" db-preset "/schema.sql") io/resource slurp)
        schemas (jdbc/execute! db-uri ["select schema_name from information_schema.schemata where catalog_name = 'penkala_dev' and schema_name not like 'pg_%' and schema_name not like 'information_schema'"] {:builder-fn rs/as-unqualified-lower-maps})]
    (doseq [s schemas]
      (jdbc/execute! db-uri [(str "DROP SCHEMA " (:schema_name s) " CASCADE")]))
    (jdbc/execute! db-uri [sql])))

(defn reset-db-fixture [db-preset f]
  (reset-db db-preset)
  (binding [*env* (db/get-env db-uri)]
    (f)))