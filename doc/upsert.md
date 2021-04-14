# Upsert

## ON CONFLICT (...) DO NOTHING

```clojure
(require '[com.verybigthings.penkala.relation :as r])

(require '[com.verybigthings.penkala.next-jdbc :refer [insert!]])

(def users-ins (r/->insertable (:users *env*)))

(insert! *env*users-ins {:is-admin true :username "jon.doe"})

=> #:users{:is-admin true, :username "jon.doe", :id 1}

(insert! *env*
 (-> users-ins
  (r/on-conflict-do-nothing))
 {:is-admin true :username "jon.doe"})

=> nil

;; specify conflict_target

(insert! *env*
  (-> users-ins
    (r/on-conflict-do-nothing [:username]))
  {:is-admin true :username "jon.doe"})

=> nil

(insert! *env*
  (-> users-ins
    (r/on-conflict-do-nothing [:on-constraint "users_pkey"]))
  {:id 1 :is-admin true :username "foo.bar"})

=> nil

;; with constraint inference without passing explicit constraint name

(insert! *env*
  (-> users-ins
    (r/on-conflict-do-nothing [:username] [:= :id 1]))
  {:is-admin true :username "john.doe"})

=> nil
```

## ON CONFLICT (...) DO UPDATE

```clojure
(insert! *env*
  (-> users-ins
    (r/on-conflict-do-update
      [:username]
      {:username [:concat :excluded/username "1"]}))
  {:is-admin true :username "jon.doe"})

;; with explicit constraint name

(insert! *env*
  (-> users-ins
    (r/on-conflict-do-update
      [:on-constraint "users_pkey"]
      {:username [:concat :excluded/username "1"] :id 1}))
  {:id 1 :is-admin true :username "jon.doe"})
```