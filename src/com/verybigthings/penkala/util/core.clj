(ns com.verybigthings.penkala.util.core)

(defn str-quote [s]
  (str "\"" (if (keyword? s) (name s) s) "\""))

(defn select-keys-with-default [m ks default]
  (reduce
    (fn [acc k]
      (assoc acc k (get m k default)))
    {}
    ks))

(defn as-vec [val]
  (if (sequential? val) (vec val) [val]))