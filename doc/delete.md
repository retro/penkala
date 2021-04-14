# Delete

## Simple using keywords

```clojure
(require '[com.verybigthings.penkala.next-jdbc :refer [insert! delete!]])

;; prepare some rows in the DB

(insert! *env* :users [{:is-admin false :username "jon.doe"} {:is-admin true :username "john.smith"}])

;; delete all rows

(delete! *env* :users)

=> [#:users{:is-admin nil, :username "jon.doe", :id 1} #:users{:is-admin true, :username "john.smith", :id 2}]

(delete! *env* :users)

=> nil
```

## Defining deletable record and using WHERE clause

```clojure
(require '[com.verybigthings.penkala.relation :as r])

(def users-del (r/->deletable (:users *env*)))

(delete! *env* (r/where users-del [:= :id 1]))

=> [#:users{:is-admin nil, :username "jon.doe", :id 1}]

(delete! *env* (r/where users-del [:= :id 1]))

=> nil
```

## Returning value

```clojure
(delete! *env*
  (-> users-del
    (r/where [:= :id 1])
    (r/returning nil)))

=> #:next.jdbc{:update-count 1}

(delete! *env*
  (-> users-del
    (r/where [:= :id 1])
    (r/returning [:id])))

=> [#:users{:id 1}]
```

## With USING clause

```clojure
(let [users-rel (:users *env*)
  		del-users (-> users-del
									(r/using (r/where users-rel [:= :id 1]) :other-users)
								 	(r/where [:= :id :other-users/id]))]
  (delete! *env* del-users))

=> [#:users{:is-admin nil, :username "jon.doe", :id 1}]

;; multiple USING

(let [users-rel (:users *env*)
  		del-users (-> users-del
									(r/using (r/where users-rel [:= :id 1]) :other-users-1)
									(r/using (r/where users-rel [:= :id 1]) :other-users-2)
								 	(r/where [:and
								 	          [:= :id :other-users-1/id]
								 	          [:= :id :other-users-2/id]]))]
  (delete! *env* del-users))

=> [#:users{:is-admin nil, :username "jon.doe", :id 1}]
```