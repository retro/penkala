(ns com.verybigthings.penkala.util.decompose
  (:require [clojure.spec.alpha :as s]
            [com.verybigthings.penkala.util.core :refer [as-vec path-prefix-join]]
            [camel-snake-kebab.core :refer [->kebab-case-string ->kebab-case-keyword]]))

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

(defn assoc-columns [acc schema renames row]
  (let [ns (:namespace schema)]
    (reduce-kv
      (fn [acc' k v]
        (if (contains? row k)
          (let [col-name (if ns (keyword (name ns) (name v)) v)]
            (assoc acc' col-name (get row k)))
          acc'))
      acc
      renames)))

(declare build)

(defn assoc-descendants [acc schema children-schemas idx row]
  (let [ns (:namespace schema)]
    (reduce-kv
      (fn [m k v]
        (let [descendant (build (get acc k) v idx row)]
          (if descendant
            (let [col-name (if ns (keyword (name ns) (name k)) k)]
              (assoc m col-name descendant))
            m)))
      acc
      children-schemas)))

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
                      (assoc-columns schema renames row)
                      (assoc-descendants schema schemas idx row))]
        (assoc acc id current)))))

(defn transform [schema mapping]
  (let [decompose-to (get schema :decompose-to :coll)
        ns (:namespace schema)
        pk (:pk schema)
        {:keys [schemas]} (expand-columns (:columns schema))
        transformed (reduce-kv
                      (fn [acc k row]
                        (let [transformed
                              (reduce-kv
                                (fn [row' k k-schema]
                                  (let [col-name (if ns (keyword (name ns) (name k)) k)]
                                    (assoc row' col-name (transform k-schema (get row col-name)))))
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

(defn get-prefixed-col-name [path-prefix col-name]
  (->> (conj path-prefix (->kebab-case-string col-name))
    (mapv name)
    path-prefix-join
    keyword))

(defn infer-schema
  ([relation] (infer-schema relation nil []))
  ([relation overrides] (infer-schema relation overrides []))
  ([relation overrides path-prefix]
   (let [pk                  (->> (as-vec (get-in relation [:spec :pk]))
                               (mapv #(get-prefixed-col-name path-prefix %)))
         default-namespace (:namespace overrides)
         namespace (if (nil? default-namespace)
                     (->kebab-case-string (get-in relation [:spec :name]))
                     default-namespace)
         decompose-to (or (:decompose-to overrides) :coll)
         columns (reduce
                   (fn [acc col-name]
                     (assoc acc (get-prefixed-col-name path-prefix col-name) col-name))
                   {}
                   (:projection relation))
         columns-with-joined (reduce-kv
                               (fn [acc alias join]
                                 (let [join-relation  (:relation join)
                                       join-overrides (get overrides alias)
                                       join-path (conj path-prefix alias)
                                       join-schema    (infer-schema join-relation join-overrides join-path)]
                                   (assoc acc alias join-schema)))
                               columns
                               (:joins relation))]
     {:pk pk
      :decompose-to decompose-to
      :namespace namespace
      :columns columns-with-joined})))