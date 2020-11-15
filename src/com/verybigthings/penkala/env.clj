(ns com.verybigthings.penkala.env)

(defn with-schema-rename
  ([old-schema-name new-schema-name] (with-schema-rename {} old-schema-name new-schema-name))
  ([env old-schema-name new-schema-name]
   (assoc-in env [::schema-renames (name old-schema-name)] (name new-schema-name))))

(defn with-current-schema
  ([current-schema] (with-current-schema {} current-schema))
  ([env current-schema]
   (assoc env ::schema current-schema)))

(defn with-db
  ([db] (with-db {} db))
  ([env db]
   (assoc env ::db db)))