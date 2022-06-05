(ns com.verybigthings.penkala.relation
  (:require [clj-kondo.hooks-api :as api]))

(defn as-cte [{:keys [node]}]
  (let [children (:children node)
        children-count (count children)]

    (cond
      (= 2 children-count)
      {:node node}

      (= 3 children-count)
      (let [[_ body recursive-children] children
            [recursive-op recursive-binding recursive-body] (:children recursive-children)]
        (when-not (contains? #{'union 'union-all} (api/sexpr recursive-op))
          (throw (ex-info "as-cte supports only union and union-all operations" {})))
        {:node (api/list-node
                (list*
                 (api/token-node 'do)
                 body
                 (api/list-node
                  (list*
                   (api/token-node 'fn)
                   recursive-binding
                   recursive-body))))})
      :else
      (throw (ex-info "as-cte macro has wrong form" {})))))