(ns com.verybigthings.penkala.test-helpers
  (:require [next.jdbc :as jdbc]
            [clojure.java.io :as io]
            [next.jdbc.result-set :as rs]
            [com.verybigthings.penkala.next-jdbc :refer [get-env]]
            [clojure.spec.test.alpha :as st]))

(def ^:dynamic *env* {})

(def db-uri "jdbc:postgresql://localhost:5432/penkala_dev?user=postgres")

(defn reset-db [db-preset]
  (let [sql     (-> (str "com/verybigthings/penkala/test_db_scripts/" db-preset "/schema.sql") io/resource slurp)
        schemas (jdbc/execute! db-uri ["select schema_name from information_schema.schemata where catalog_name = 'penkala_dev' and schema_name not like 'pg_%' and schema_name not like 'information_schema'"] {:builder-fn rs/as-unqualified-lower-maps})]
    (doseq [s schemas]
      (jdbc/execute! db-uri [(str "DROP SCHEMA " (:schema_name s) " CASCADE")]))
    (jdbc/execute! db-uri [sql])))

(defn instrument-penkala [f]
  (let [syms (-> 'com.verybigthings.penkala.relation st/enumerate-namespace st/instrumentable-syms)]
    (st/instrument syms)
    (f)
    (st/unstrument syms)))

(defn reset-db-fixture [db-preset f]
  (reset-db db-preset)
  (binding [*env* (get-env db-uri)]
    (instrument-penkala f)))

(defn pagila-db-fixture [f]
  (binding [*env* (get-env "jdbc:postgresql://localhost:5432/penkala_pagila?user=postgres")]
    (instrument-penkala f)))

