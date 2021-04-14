# Update

## Simple using keywords

```clojure
(require '[com.verybigthings.penkala.next-jdbc :refer [insert! update!]])

;; prepare some rows in the DB

(insert! *env* :users [{:is-admin false :username "jon.doe"} {:is-admin true :username "john.smith"}])

;; update all rows

(update! *env* :users {:is-admin true})

=> [#:users{:is-admin true, :username "jon.doe", :id 1} #:users{:is-admin true, :username "john.smith", :id 2}]
```

## Defining updateable record and using WHERE clause

```clojure
(require '[com.verybigthings.penkala.relation :as r])

(def users-upd (r/->updatable (:users *env*)))

(update! *env*
  (-> users-upd
    (r/where [:= :id 1]))
	{:username "foo.bar"})

=> [#:users{:is-admin true, :username "foo.bar", :id 1}]
```

## Returning value

```clojure
(update! *env*
  (-> users-upd
    (r/where [:= :id 1])
    (r/returning nil))
  {:username "jon.doe"})

=> #:next.jdbc{:update-count 1}

(update! *env*
  (-> users-upd
    (r/where [:= :id 1])
    (r/returning [:id]))
  {:username "jon.doe"})

=> [#:users{:id 1}]
```

## With FROM clause

```clojure
(let [users-rel (:users *env*)
      upd-users (-> users-upd
                  (r/from users-rel :users-rel-2)
                  (r/where [:and [:= :id 1]
                            [:= :id :users-rel-2/id]]))]
  (update! *env* upd-users {:username [:concat "new-" :users-rel-2/username]}))

=> [#:users{:is-admin true, :username "new-jon.doe", :id 1}]
```