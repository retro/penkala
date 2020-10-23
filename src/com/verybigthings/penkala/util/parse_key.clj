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

(defn parse-join [source {:keys [acc lexed] :as current}]
  (let [joins (:joins source)]
    (if (seq joins)
      (let [{:keys [tokens]} lexed]
        (cond
          (= (:name source) (first tokens))
          (let [[relation & r-tokens] tokens]
            {:acc (assoc acc :relation relation)
             :lexed (-> lexed
                      (assoc :tokens r-tokens)
                      (update :path-shape rest))})

          (= [(:schema source) (:name source)] (take 2 tokens))
          (let [[schema relation & r-tokens] tokens]
            {:acc (assoc acc :schema schema :relation relation)
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
                  (= (:alias j) t)
                  (reduced {:acc (assoc acc :alias (:alias j))
                            :lexed (-> lexed
                                     (assoc :tokens r-tokens)
                                     (update :path-shape rest))})

                  (= (:relation j) t)
                  (reduced {:acc (assoc acc :alias (:alias j) :relation t)
                            :lexed (-> lexed
                                     (assoc :tokens r-tokens)
                                     (update :path-shape rest))})

                  (= [(:schema j) (:relation j)] [t (first r-tokens)])
                  (reduced {:acc (assoc acc :alias (:alias j) :schema (:schema j) :relation (:relation j))
                            :lexed (-> lexed
                                     (assoc :tokens (rest r-tokens))
                                     (update :path-shape #(drop 2 %)))})
                  :else current')))
            (-> current
              (assoc-in [:acc :schema] (:schema source))
              (assoc-in [:acc :relation] (:name source)))
            (:joins source))))
      current)))

(defn parse-default [db {:keys [acc lexed]}]
  (let [{:keys [tokens]} lexed
        {:keys [schema relation alias]} acc
        [field & r-tokens] tokens
        is-schema-current (= schema (:schema db))
        path-elements (->> [(when (and (not alias) (not is-schema-current)) schema)
                            (or alias relation)
                            field]
                        (remove nil?))
        path              (->> path-elements (map str-quote) (str/join "."))]

    {:acc (assoc acc :path path
                     :lhs path
                     :path-elements path-elements
                     :field field)
     :lexed (assoc lexed :tokens r-tokens)}))

(defn parse-json-elements [json-as-text {:keys [acc lexed] :as current}]
  (let [{:keys [path-shape tokens]} lexed
        {:keys [path]} acc]
    (if (seq path-shape)
      (if (= 1 (count path-shape))
        (let [operator (if (or json-as-text (:json-as-text lexed)) "->>" "->")
              [t & r-tokens] tokens
              json-elements [t]
              lhs (if (= [true] path-shape) (str path operator "'" t "'") (str path operator t))]
          {:acc (assoc acc :lhs lhs :json-elements json-elements)
           :lexed (assoc lexed :tokens r-tokens)})
        (let [operator (if json-as-text "#>>" "#>")
              path-shape-length (count path-shape)
              r-tokens (drop path-shape-length tokens)
              json-elements (take path-shape-length tokens)
              lhs (str path operator "'{" (str/join "," json-elements) "}'")]
          {:acc (assoc acc :lhs lhs :json-elements json-elements)
           :lexed (assoc lexed :tokens r-tokens)}))
      current)))

(defn parse-cast [{:keys [acc lexed] :as current}]
  (if (:has-cast lexed)
    (let [{:keys [path-shape tokens]} lexed
          {:keys [lhs]} acc
          [cast & r-tokens] tokens
          lhs' (if (seq path-shape) (str "(" lhs ")::" cast) (str lhs "::" cast))]
      {:acc (assoc acc :lhs lhs')
       :lexed (assoc lexed :tokens r-tokens)})
    current))

(defn parse-key
  ([db source key] (parse-key db source key true))
  ([db source key json-as-text]
   (let [acc {:alias nil :schema nil :relation nil :lhs nil}
         res   (->> {:acc acc :lexed (lex key)}
                 (parse-join source)
                 (parse-default db)
                 (parse-json-elements json-as-text)
                 (parse-cast))
         {:keys [acc lexed]} res
         tokens (:tokens lexed)
         remainder (when (seq tokens) (->> tokens (str/join " ") str/lower-case))]

     (-> acc
       (assoc :relation (or (:relation acc) (:alias acc) (:name source))
              :remainder remainder)))))

(defn with-appendix
  ([db source key appendix] (with-appendix db source key appendix nil nil))
  ([db source key appendix value offset]
   (let [predicate (parse-key db source key)
         remainder (:remainder predicate)
         appended  (appendix (or remainder "="))]
     (assoc predicate :offset offset :value value :params [] :appended appended))))