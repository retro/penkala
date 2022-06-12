(ns com.verybigthings.penkala.decomposition
  (:require [clojure.spec.alpha :as s]
            [com.verybigthings.penkala.util :refer [as-vec path-prefix-join col->alias]]
            [clojure.set :as set]
            [clojure.string :as str]))

(defmulti coerce-embedded-value (fn [pg-type _] pg-type))
(defmethod coerce-embedded-value :default [_ value] value)

(defn coerce-embedded-row [types row]
  (->> row
       (map (fn [[k value]]
              (let [pg-type (get types (name k))]
                [k (coerce-embedded-value pg-type value)])))
       (into {})))

(defrecord DecompositionSchema [pk decompose-to namespace columns])

(s/def ::decompose-to #{:indexed-by-pk :coll :map :parent :omit})

(s/def ::schema-map
  (s/map-of
   keyword?
   (s/or
    :keyword keyword?
    :schema ::schema)))

(s/def ::schema-vec
  (s/coll-of
   (s/or
    :keyword keyword?
    :schema-map ::schema-map)
   :kind vector?))

(s/def ::schema
  (s/or
   :schema-map ::schema-map
   :schema-vec ::schema-vec))

(s/def ::pk
  (s/or
   :single keyword?
   :multiple (s/coll-of keyword?)))

(s/def ::namespace
  (s/or :string string? :keyword keyword? :boolean boolean?))

(s/def ::keep-duplicates?
  boolean?)

(s/def ::keep-nil?
  boolean?)

(s/def ::embedded?
  boolean?)

(s/def ::schema
  (s/keys
   :opt-un [::decompose-to ::pk ::schema ::keep-duplicates? ::keep-nil? ::embedded? ::namespace]))

(declare process-schema)

(defn process-schema-columns [schema]
  (let [columns             (:schema schema)
        ns-name             (when-let [ns (:namespace schema)] (name ns))
        rename              (if ns-name (fn [k] (keyword ns-name (name k))) identity)
        process-map-columns (fn [schema columns]
                              (reduce-kv
                               (fn [acc k v]
                                 (if (map? v)
                                   (assoc-in acc [:schemas (rename k)] (process-schema k v))
                                   (assoc-in acc [:renames (rename k)] v)))
                               schema
                               columns))]
    (if (map? columns)
      (process-map-columns schema columns)
      (reduce
       (fn [acc v]
         (if (map? v)
           (process-map-columns acc v)
           (assoc-in acc [:renames v] (rename v))))
       schema
       columns))))

(defn process-embedded-schema [schema column]
  (cond
    (and (:embedded? schema) column)
    (-> schema (dissoc :embedded?) (assoc :embedded column))

    (and (:embedded? schema) (not column))
    (throw (ex-info "Root schema can't have :embedded? true" {:schema schema}))

    :else
    schema))

(defn process-schema- [column schema]
  (-> schema
      process-schema-columns
      (process-embedded-schema column)
      (update :pk as-vec)))

(def process-schema (memoize process-schema-))

(defn assoc-columns [acc renames row]
  (reduce-kv
   (fn [acc' k v]
     (if (contains? row v)
       (assoc acc' k (get row v))
       acc'))
   acc
   renames))

(declare build)

(defn assoc-descendants [acc schemas idx row]
  (reduce-kv
   (fn [m k v]
     (if-let [embedded (:embedded v)]
       (let [{:keys [data types]} (get row embedded)
             processed (reduce
                        (fn [acc [idx row]]
                          (->> row
                               (coerce-embedded-row types)
                               (build acc v idx)))
                        {}
                        (map-indexed (fn [idx v] [idx v]) (as-vec data)))]
         (assoc m k processed))
       (let [descendant (build (get acc k) v idx row)]
         (if descendant
           (assoc m k descendant)
           m))))
   acc
   schemas))

(defn get-row-id [schema row]
  (let [pk (:pk schema)
        is-composite-pk (< 1 (count pk))
        id (if is-composite-pk
             (mapv #(get row %) pk)
             (get row (first pk)))]
    (when (or (not is-composite-pk)
              (and is-composite-pk (->> pk (remove nil?) seq)))
      id)))

(def set-conj (fnil conj #{}))

(defn build [acc schema idx row]
  (let [id (get-row-id schema row)
        {:keys [renames schemas keep-duplicates?]} schema
        current (-> (get acc id {})
                    (vary-meta update ::idx #(if keep-duplicates? (set-conj % idx) (or % #{idx})))
                    (assoc-columns renames row)
                    (assoc-descendants schemas idx row))]
    (cond
      id (assoc acc id current)
      (:keep-nil? schema) (assoc acc [idx row] current)
      :else acc)))

(defn expand-transformed-coll [coll]
  (let [indexed-coll (for [item coll
                           idx (-> item meta ::idx)]
                       [idx item])]
    (->> indexed-coll
         (sort-by first)
         (mapv second))))

(defn transform [schema mapping]
  (let [decompose-to (get schema :decompose-to :coll)
        processor (get schema :processor identity)
        schemas      (:schemas schema)
        transformed  (reduce-kv
                      (fn [acc k row]
                        (let [transformed
                              (reduce-kv
                               (fn [row' k k-schema]
                                 (let [transformed-child (transform k-schema (get row k))]
                                   (if (= :parent (:decompose-to k-schema))
                                     (-> row'
                                         (dissoc k)
                                         (merge transformed-child))
                                     (assoc row' k transformed-child))))
                               row
                               schemas)
                              transformed' (processor transformed)]
                          (cond
                            (= :coll decompose-to)
                            (conj acc transformed')

                            (= :indexed-by-pk decompose-to)
                            (assoc acc k transformed')

                             ;; decompose to :map and :parent should just return the map
                            :else
                            transformed')))
                      (if (= :coll decompose-to) [] {})
                      mapping)]
    (if (= :coll decompose-to)
      (expand-transformed-coll transformed)
      transformed)))



(defn decompose
  "Decomposes the data based on the schema."
  [schema data]
  (when (and (seq schema) (seq data))
    (let [schema' (process-schema nil schema)
          mapping (reduce
                   (fn [acc [idx row]]
                     (build acc schema' idx row))
                   {}
                   (map-indexed (fn [idx v] [idx v]) (as-vec data)))]
      (transform schema' mapping))))

(s/fdef decompose
  :args (s/cat
         :schema ::schema
         :data (s/or :map map? :coll sequential? :nil nil?))
  :ret (s/or :map? map? :coll sequential? :nil nil?))

(defn get-prefixed-col-name [path-prefix col-name]
  (->> (conj path-prefix (col->alias col-name))
       (mapv name)
       path-prefix-join
       keyword))

(defn get-rel-pk [rel]
  (let [pk-ids (:pk rel)
        pk-aliases (mapv #(get-in rel [:ids->aliases %]) pk-ids)
        projection (:projection rel)]
    (if (and (seq pk-aliases) (set/subset? (set pk-aliases) projection))
      pk-aliases
      (vec (sort projection)))))

(defn infer-flat-schema
  ([relation overrides path-prefix]
   (let [columns (infer-flat-schema relation overrides path-prefix {})
         processor (get overrides :processor identity)]
     (map->DecompositionSchema
      {:pk (-> (vals columns) sort vec)
       :namespace false
       :schema columns
       :decompose-to :coll
       :processor processor})))
  ([relation overrides path-prefix schema]
   (let [with-columns (reduce
                       (fn [acc col-name]
                         (let [col-ns (when (seq path-prefix) (str/join "." (map name path-prefix)))
                               col-name' (if col-ns (keyword col-ns (name col-name)) col-name)]
                           (assoc acc col-name' (get-prefixed-col-name path-prefix col-name))))
                       schema
                       (:projection relation))]
     (reduce-kv
      (fn [acc alias join]
        (infer-flat-schema (:relation join) overrides (conj path-prefix alias) acc))
      with-columns
      (:joins relation)))))

(defn infer-schema
  "Infers the decomposition schema from a relation."
  ([relation] (infer-schema relation nil []))
  ([relation overrides] (infer-schema relation overrides []))
  ([relation overrides path-prefix]
   (cond
     (false? overrides)
     (infer-flat-schema relation overrides path-prefix)

     (= DecompositionSchema (type overrides))
     overrides

     :else
     (let [pk                  (->> (or (:pk overrides) (get-rel-pk relation))
                                    as-vec
                                    (mapv #(get-prefixed-col-name path-prefix %)))
           default-namespace   (:namespace overrides)
           namespace           (if (nil? default-namespace)
                                 (get-in relation [:spec :namespace])
                                 default-namespace)
           decompose-to        (get overrides :decompose-to :coll)
           processor           (get overrides :processor identity)
           keep-nil?           (get overrides :keep-nil? false)
           keep-duplicates?    (get overrides :keep-duplicates? false)
           columns             (reduce
                                (fn [acc col-name]
                                  (assoc acc col-name (get-prefixed-col-name path-prefix col-name)))
                                {}
                                (:projection relation))
           columns-with-joined (reduce-kv
                                (fn [acc alias join]
                                  (let [join-projection (:projection join)
                                        join-relation  (update (:relation join) :projection #(or join-projection %))
                                        join-overrides (get-in overrides [:schema alias])
                                        join-path      (conj path-prefix alias)
                                        join-type      (:type join)]
                                    (if (-> join-relation :projection seq)
                                      ;; If we encounter a join that might have nils on the left side
                                      ;; we switch to the flat decomposition, where we just namespace all columns
                                      ;; to avoid duplicates. The namespacing behavior is different than the default
                                      ;; because it's using join alias as a namespace instead of the relation name
                                      ;; to ensure that joining a same relation multiple times is not overriding values
                                      (let [join-schema (if (contains? #{:left :left-lateral :inner :inner-lateral} join-type)
                                                          (infer-schema join-relation join-overrides join-path)
                                                          (infer-flat-schema join-relation join-overrides join-path))]
                                     ;; First we build join schema and then check if this level should be omitted.
                                     ;; If it should, we still need to pick up any joins on the levels below it.
                                     ;; This is not super efficient as we'll iterate through some columns multiple
                                     ;; times, but works until a better way is figured out.
                                        (if (= :omit (:decompose-to join-schema))
                                          (let [join-relation-projection (:projection join-relation)]
                                            (reduce-kv
                                             (fn [acc' col col-schema]
                                               (if (contains? join-relation-projection col)
                                                 acc'
                                                 (assoc acc col col-schema)))
                                             acc
                                             (:schema join-schema)))
                                          (assoc acc alias join-schema)))
                                      acc)))
                                columns
                                (:joins relation))
           columns-with-joined-and-embeds (reduce
                                           (fn [acc alias]
                                             (let [col-id (get-in relation [:aliases->ids alias])
                                                   {:keys [type relation]} (get-in relation [:columns col-id])]
                                               (if (= :embed type)
                                                 (let [overrides (get-in overrides [:schema alias])
                                                       schema (infer-schema relation overrides [])]
                                                   (assoc acc alias (assoc schema :embedded? true)))
                                                 acc)))
                                           columns-with-joined
                                           (:projection relation))]
       (map->DecompositionSchema
        {:pk pk
         :decompose-to decompose-to
         :namespace namespace
         :schema columns-with-joined-and-embeds
         :processor processor
         :keep-nil? keep-nil?
         :keep-duplicates? keep-duplicates?})))))