(ns com.verybigthings.penkala.next-jdbc
  (:require [next.jdbc.result-set :as rs])
  (:import [java.sql Array]))

(extend-protocol rs/ReadableColumn
  Array
  (read-column-by-label [^Array v _]    (vec (.getArray v)))
  (read-column-by-index [^Array v _ _]  (vec (.getArray v))))