(ns com.verybigthings.penkala.statement.select
  ^{:doc "This namespace is DEPRECATED. Please use the `com.verybigthings.penkala.statement` namespace"}
  (:require [com.verybigthings.penkala.statement :refer [format-select-query format-select-query-without-params-resolution]]))

(def format-query-without-params-resolution format-select-query-without-params-resolution)
(def format-query format-select-query)