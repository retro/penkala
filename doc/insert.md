# Insert

## Simple using keywords

```clojure
(require '[com.verybigthings.penkala.next-jdbc :refer [insert!]])

(insert! *env* :users {:is-admin true :username "jon.doe"})

=> #:users{:is-admin true, :username "jon.doe", :id 1}

(insert! *env* :users {:is-admin true :username "jon.doe"})

=> Execution error (PSQLException) at org.postgresql.core.v3.QueryExecutorImpl/receiveErrorResponse (QueryExecutorImpl.java:2553).
ERROR: duplicate key value violates unique constraint "users_username_key"
  Detail: Key (username)=(jon.doe) already exists.
```

## Multiple rows

Pass a vector of maps to create multiple rows:

```clojure
(insert! *env* :users [{:is-admin true :username "foo.bar"} {:is-admin true :username "john.smith"}])

=> [#:users{:is-admin true, :username "foo.bar", :id 2} #:users{:is-admin true, :username "john.smith", :id 3}]
```

As can be seen, a vector of results is returned.

## Defining insertable record

```clojure
(require '[com.verybigthings.penkala.relation :as r])

(def users-ins (r/->insertable (:users *env*)))

(insert! *env* users-ins {:is-admin true :username "antoher.john.smith"})

=> #:users{:is-admin true, :username "antoher.john.smith", :id 4}
```

## Returning value

```clojure
(insert! *env* (r/returning users-ins [:id]) {:is-admin true :username "antoher.jon.doe"})

=> #:users{:id 5}

(insert! *env* (r/returning users-ins nil) {:is-admin true :username "antoher.foo.bar"})

=> #:next.jdbc{:update-count 1}
```