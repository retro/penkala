(ns com.verybigthings.penkala.util.parse-key
  (:require [clojure.string :as str]
            [com.verybigthings.penkala.util.core :refer [str-quote]]))

(defn lex [key]
  (loop [chars (-> key str/trim seq)
         buffer []
         acc {:tokens [] :path-shape [] :in-quotation false :has-cast false :json-as-text false}]
    (if (seq chars)
      (let [[c & r-chars] chars
            {:keys [in-quotation has-cast]} acc]
        (if (and in-quotation (not= \" c))
          (recur r-chars (conj buffer c) acc)
          (case c
            \"
            (if in-quotation
              (recur r-chars [] (-> acc
                                  (update :tokens conj buffer)
                                  (assoc :in-quotation false)))
              (recur r-chars [] (assoc acc :in-quotation true)))

            \:
            (if (not has-cast)
              (recur r-chars [] (-> acc
                                  (update :tokens conj buffer)
                                  (assoc :json-as-text true :has-cast true)))
              (recur r-chars [] acc))

            \.
            (recur r-chars [] (-> acc
                                (update :tokens conj buffer)
                                (update :path-shape conj true)))

            \[
            (recur r-chars [] (-> acc
                                (update :tokens conj buffer)
                                (update :path-shape conj false)))

            \]
            (recur r-chars [] (update acc :tokens conj buffer))

            (\space \tab \return \newline)
            (recur r-chars [] (update acc :tokens conj buffer))

            (recur r-chars (conj buffer c) acc))))
      (let [tokens (reduce
                      (fn [acc t]
                        (let [s (->> t (str/join "") str/trim)]
                          (if (seq s)
                            (conj acc s)
                            acc)))
                      []
                     (conj (:tokens acc) buffer))]
        (-> acc
          (select-keys [:path-shape :json-as-text :has-cast])
          (assoc :tokens tokens))))))

(defn parse-join [{:keys [acc lexed] :as current} source]
  (let [joins (:relation/joins source)]
    (if (seq joins)
      (let [{:keys [tokens]} lexed]
        (cond
          (= (:relation/name source) (first tokens))
          (let [[relation & r-tokens] tokens]
            {:acc (assoc acc :relation/name relation)
             :lexed (-> lexed
                      (assoc :tokens r-tokens)
                      (update :path-shape rest))})

          (= [(:db/schema source) (:relation/name source)] (take 2 tokens))
          (let [[schema relation & r-tokens] tokens]
            {:acc (assoc acc :db/schema schema :relation/name relation)
             :lexed (-> lexed
                      (assoc :tokens r-tokens)
                      (update :path-shape #(drop 2 %)))})
          :else
          (reduce-kv
            (fn [current' _ j]
              (let [{:keys [acc lexed]} current
                    {:keys [tokens]} lexed
                    [t & r-tokens] tokens]
                (cond
                  (= (:relation/alias j) t)
                  (reduced {:acc (assoc acc :relation/alias (:relation/alias j))
                            :lexed (-> lexed
                                     (assoc :tokens r-tokens)
                                     (update :path-shape rest))})

                  (= (:relation/name j) t)
                  (reduced {:acc (assoc acc :relation/alias (:relation/alias j)
                                            :relation/name t)
                            :lexed (-> lexed
                                     (assoc :tokens r-tokens)
                                     (update :path-shape rest))})

                  (= [(:db/schema j) (:relation/name j)] [t (first r-tokens)])
                  (reduced {:acc (assoc acc :relation/alias (:relation/alias j)
                                            :db/schema (:db/schema j)
                                            :relation/name (:relation/name j))
                            :lexed (-> lexed
                                     (assoc :tokens (rest r-tokens))
                                     (update :path-shape #(drop 2 %)))})
                  :else current')))
            (-> current
              (assoc-in [:acc :db/schema] (:db/schema source))
              (assoc-in [:acc :relation/name] (:relation/name source)))
            joins)))
      current)))

(defn parse-default [{:keys [acc lexed]} source]
  (let [{:keys [tokens]} lexed
        [field & r-tokens] tokens]
    {:acc (assoc acc :query/field field)
     :lexed (assoc lexed :tokens r-tokens)}))

(defn parse-json-elements [{:keys [acc lexed] :as current} json-as-text]
  (let [{:keys [path-shape tokens]} lexed]
    (if (seq path-shape)
      (if (= 1 (count path-shape))
        (let [operator (if (or json-as-text (:json-as-text lexed)) "->>" "->")
              [t & r-tokens] tokens
              json-elements [t]]
          {:acc (assoc acc :query/json {:elements json-elements
                                        :operator operator
                                        :path-wrap (when (= [true] path-shape) :quote)})
           :lexed (assoc lexed :tokens r-tokens)})
        (let [operator (if json-as-text "#>>" "#>")
              path-shape-length (count path-shape)
              r-tokens (drop path-shape-length tokens)
              json-elements (take path-shape-length tokens)]
          {:acc (assoc acc :query/json {:elements json-elements :operator operator :path-wrap :curly})
           :lexed (assoc lexed :tokens r-tokens)}))
      current)))

(defn parse-cast [{:keys [acc lexed] :as current}]
  (if (:has-cast lexed)
    (let [{:keys [tokens]} lexed
          [cast & r-tokens] tokens]
      {:acc (assoc acc :query/cast cast)
       :lexed (assoc lexed :tokens r-tokens)})
    current))

(defn parse-key
  ([source key] (parse-key source key true))
  ([source key json-as-text]
   (let [acc {:relation/alias nil :db/schema (:db/schema source) :relation/name nil}
         res   (-> {:acc acc :lexed (lex key)}
                 (parse-join source)
                 (parse-default source)
                 (parse-json-elements json-as-text)
                 (parse-cast))
         {:keys [acc lexed]} res
         tokens (:tokens lexed)
         remainder (when (seq tokens) (->> tokens (str/join " ") str/lower-case))]

     (-> acc
       (assoc :relation/name (or (:relation/name acc) (:relation/alias acc) (:relation/name source))
              :remainder remainder)))))
(defn get-path
  ([parsed] (get-path parsed {} {}))
  ([parsed source] (get-path parsed source {}))
  ([parsed source db-config]
   (let [{:relation/keys [alias name]} parsed
         {:db/keys [schema]} parsed
         {:query/keys [field]} parsed
         is-schema-current (= (:db/schema db-config) schema)

         has-joins (seq (:relation/joins source))
         is-relation-source (= name (:relation/name source))

         should-include-schema (and (not alias) (not is-schema-current))
         should-include-relation (and (not alias) (or (not is-schema-current) (not is-relation-source) has-joins))]
     (->> [(when should-include-schema schema) alias (when should-include-relation name) field]
       (remove nil?)
       (map str-quote)
       (str/join ".")))))

(defn lhs-with-json-path [path parsed]
  (if-let [json (:query/json parsed)]
    (let [json-path (str/join "," (:elements json))
          wrapped-json-path (case (:path-wrap json)
                              :curly (str "'{" json-path "}'")
                              :quote (str "'" json-path "'")
                              json-path)]
      (str path (:operator json) wrapped-json-path))
    path))

(defn lhs-with-cast [path parsed]
  (if-let [cast (:query/cast parsed)]
    ;; Should I check path-shape here?
    (if (:query/json parsed)
      (str "(" path ")::" cast)
      (str path "::" cast))
    path))

(defn get-lhs
  ([parsed] (get-lhs parsed {} {}))
  ([parsed source] (get-lhs parsed source {}))
  ([parsed source db-config]
   (let [path (get-path parsed source db-config)]
     (cond-> path

       (:query/json parsed)
       (lhs-with-json-path parsed)

       (:query/cast parsed)
       (lhs-with-cast parsed)))))

(defn with-appendix
  ([source key appendix] (with-appendix source key appendix nil))
  ([source key appendix value]
   (let [predicate (parse-key source key)
         remainder (:remainder predicate)
         appended  (appendix (or remainder "="))]
     (assoc predicate :value value :params [] :appended appended))))