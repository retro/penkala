(ns com.verybigthings.penkala.util.decompose)

(def nil-pks-err)

(defn as-coll [val]
  (if (sequential? val) val [val]))

(defn assoc-columns [acc columns row]
  (if (map? columns)
    (reduce-kv
      (fn [acc' k v]
        (if (contains? row k)
          (assoc acc' v (get row k))
          acc'))
      acc
      columns)
    (reduce
      (fn [acc' k]
        (if (contains? row k)
          (assoc acc' k (get row k))
          acc'))
      acc
      columns)))

(declare build)

(defn assoc-descendants [acc schema idx row]
  (reduce-kv
    (fn [m k v]
      (if (contains? #{:pk :columns :decompose-to} k)
        m
        (let [descendant (build (get acc k) v idx row)]
          (if descendant
            (assoc m k descendant)
            m))))
    acc
    schema))

(defn build [acc schema idx row]
  (let [pks (-> schema :pk as-coll)
        id (select-keys row pks)]
    (if (every? nil? (vals id))
      acc
      (let [current (-> (get acc id {})
                      (vary-meta update ::idx #(or % idx))
                      (assoc-columns (:columns schema) row)
                      (assoc-descendants schema idx row))]
        (assoc acc id current)))))

(defn transform [schema mapping]
  (let [decompose-to (get schema :decompose-to :coll)
        pk (:pk schema)
        transformed (reduce-kv
                      (fn [acc k row]
                        (let [transformed
                              (reduce-kv
                                (fn [row' k k-schema]
                                  (if (contains? #{:pk :columns :decompose-to} k)
                                    row
                                    (assoc row' k (transform k-schema (get row k)))))
                                row
                                schema)]
                          (cond
                            (= :coll decompose-to)
                            (conj acc transformed)

                            (= :dict decompose-to)
                            (let [id (if (coll? pk)
                                       (mapv (fn [pk] (get k pk)) pk)
                                       (get k pk))]
                              (assoc acc id transformed))

                            :else
                            transformed)))
                      (if (= :coll decompose-to) [] {})
                      mapping)]
    (if (= :coll decompose-to)
      (->> transformed
        (sort-by #(-> % meta ::idx))
        vec)
      transformed)))

(defn decompose [schema data]
  (when (and (seq schema) (seq data))
    (let [pks (-> schema :pk as-coll)
          mapping (reduce
                    (fn [acc [idx row]]
                      (assert (not (every? nil? (-> row (select-keys pks) vals))) nil-pks-err)
                      (build acc schema idx row))
                    {}
                    (map-indexed (fn [idx v] [idx v]) (as-coll data)))]
      (transform schema mapping))))