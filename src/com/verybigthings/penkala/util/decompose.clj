(ns com.verybigthings.penkala.util.decompose
  (:require [clojure.spec.alpha :as s]
            [com.verybigthings.penkala.util.core :refer [as-vec]]))

(s/def ::decompose-to #{:dict :coll :map})

(s/def ::column-map
  (s/map-of
    keyword?
    (s/or
      :keyword keyword?
      :schema ::schema)))

(s/def ::column-vec
  (s/coll-of
    (s/or
      :keyword keyword?
      :column-map ::column-map)
    :kind vector?))

(s/def ::columns
  (s/or
    :column-map ::column-map
    :column-vec ::column-vec))

(s/def ::pk
  (s/or
    :single keyword?
    :multiple (s/coll-of keyword?)))

(s/def ::schema
  (s/keys
    :req-un [::pk ::columns]
    :opt-un [::decompose-to]))

(def nil-pks-err)

(defn assoc-columns [acc renames row]
  (reduce-kv
    (fn [acc' k v]
      (if (contains? row k)
        (assoc acc' v (get row k))
        acc'))
    acc
    renames))

(declare build)

(defn assoc-descendants [acc schemas idx row]
  (reduce-kv
    (fn [m k v]
      (let [descendant (build (get acc k) v idx row)]
        (if descendant
          (assoc m k descendant)
          m)))
    acc
    schemas))

(defn expand-columns
  ([columns] (expand-columns {:renames {} :schemas {}} columns))
  ([expanded columns]
   (if (map? columns)
     (reduce-kv
       (fn [acc k v]
         (if (map? v)
           (assoc-in acc [:schemas k] v)
           (assoc-in acc [:renames k] v)))
       expanded
       columns)
     (reduce
       (fn [acc v]
         (if (map? v)
           (expand-columns acc v)
           (assoc-in acc [:renames v] v)))
       expanded
       columns))))

(defn build [acc schema idx row]
  (let [pks (-> schema :pk as-vec)
        id (select-keys row pks)
        {:keys [renames schemas]} (expand-columns (:columns schema))]
    (if (every? nil? (vals id))
      acc
      (let [current (-> (get acc id {})
                      (vary-meta update ::idx #(or % idx))
                      (assoc-columns renames row)
                      (assoc-descendants schemas idx row))]
        (assoc acc id current)))))

(defn transform [schema mapping]
  (let [decompose-to (get schema :decompose-to :coll)
        pk (:pk schema)
        decompose-name (:decompose/name schema)
        {:keys [schemas]} (expand-columns (:columns schema))
        transformed (reduce-kv
                      (fn [acc k row]
                        (let [transformed
                              (reduce-kv
                                (fn [row' k k-schema]
                                  (assoc row' k (transform k-schema (get row k))))
                                row
                                schemas)]
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
    (s/assert ::schema schema)
    (let [pks (-> schema :pk as-vec)
          mapping (reduce
                    (fn [acc [idx row]]
                      (assert (not (every? nil? (-> row (select-keys pks) vals))) nil-pks-err)
                      (build acc schema idx row))
                    {}
                    (map-indexed (fn [idx v] [idx v]) (as-vec data)))]
      (transform schema mapping))))

(s/fdef decompose
  :args (s/cat ::schema (s/or :map map? :coll sequential?))
  :ret (s/or :map? map? :coll sequential?))