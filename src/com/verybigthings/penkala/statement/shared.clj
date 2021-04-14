(ns com.verybigthings.penkala.statement.shared
  (:require [com.verybigthings.penkala.util :refer [q]]
            [com.verybigthings.penkala.env :as env]))

(defn make-rel-alias-prefix [_]
  (str (gensym "sq_")))

(defn get-rel-alias-with-prefix [env rel-alias]
  (if-let [rel-alias-prefix (-> env ::relation-alias-prefix last)]
    (str rel-alias-prefix "__" rel-alias)
    rel-alias))

(defn get-rel-alias [rel]
  (get-in rel [:spec :name]))

(defn get-rel-schema [env rel]
  (let [current-schema (::env/schema env)
        schema-renames (::env/schema-renames env)
        original-schema (get-in rel [:spec :schema])
        renamed-schema (if-let [r (get schema-renames original-schema)] r original-schema)]
    (when (not= current-schema renamed-schema)
      renamed-schema)))

(defn get-schema-qualified-relation-name [env rel]
  (let [rel-name       (get-in rel [:spec :name])]
    (if-let [rel-schema (get-rel-schema env rel)]
      (str (q rel-schema) "." (q rel-name))
      (q rel-name))))
