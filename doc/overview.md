# Overview

The intention of this documentation is to explain how to use Penkala through examples. For detailed information regarding a specific function check [cljdoc](https://cljdoc.org/versions/com.verybigthings/penkala) and code/tests.

## Composing queries

We'll start with generating a simple ```SELECT``` statement towards ```users``` table by using [keywords](https://clojure.org/reference/data_structures#Keywords) and ```com.verybigthings.penkala.relation/get-select-query``` function:

```clojure
;; ignored by seancorfield/readme

(require '[com.verybigthings.penkala.relation :as r])

(r/get-select-query (-> *env* :users) {})

=> ["SELECT \"users\".\"id\" AS \"id\", \"users\".\"is_admin\" AS \"is-admin\", \"users\".\"username\" AS \"username\" FROM \"public\".\"users\" AS \"users\""]
```

As you can see, ```get-select-query``` takes database configuration from the ```environment```, checks if ```users``` table exists and returns a query which can be passed to next-jdbc to retrieve the data.```["SELECT  FROM \"\" AS \"\""]``` is returned in case the table does not exist.

Composing queries can be achieved in a more explicit way by using relation spec:

```clojure
(require '[com.verybigthings.penkala.relation :as r])

(def users-spec {:name "users"
                 :columns ["id" "username" "is_admin"]
                 :pk ["id"]})

(def users-rel (r/spec->relation users-spec))

(r/get-select-query users-rel {})

=> ["SELECT \"users\".\"id\" AS \"id\", \"users\".\"is_admin\" AS \"is-admin\", \"users\".\"username\" AS \"username\" FROM \"users\" AS \"users\""]
```

```users-spec``` describes table name, columns, primary key and schema. It's used by ```spec->relation``` to create a relation wich is then used to generate the query. Note that ```environment``` isn't used in this case.


### Getting the data

Penkala provides the integration with the [next.jdbc](https://github.com/seancorfield/next-jdbc/) library. To retrieve the data, we'll use ```com.verybigthings.penkala.next-jdbc/select!``` and ```com.verybigthings.penkala.next-jdbc/select-one!```  functions which will return a map of relations or a relation respectively:

```clojure
;; ignored by seancorfield/readme

(select! *env* users-rel)
=> [#:users{:is-admin false, :username test@test.com, :id 1} #:users{:is-admin true, :username admin@test.com, :id 2}]

(select-one! *env* (-> users-rel (r/where [:= 1 :id])))
=> #:users{:is-admin false, :username test@test.com, :id 1}

(select! *env* :users)
=> [#:users{:is-admin false, :username test@test.com, :id 1} #:users{:is-admin true, :username admin@test.com, :id 2}]

(select-one! *env* (-> *env* :users (r/where [:= 1 :id])))
=> #:users{:is-admin false, :username test@test.com, :id 1}
```

In case a relation isn't recognized from the keyword Penkala will throw an eception.


### Specifying a schema

Schema can be specified explicitly through relation spec:

```clojure
(def users-spec-with-schema {:name "users"
                 :columns ["id" "username" "is_admin"]
                 :pk ["id"]
                 :schema "foo"})

(r/get-select-query (r/spec->relation users-spec-with-schema) {})

=> ["SELECT \"users\".\"id\" AS \"id\", \"users\".\"is_admin\" AS \"is-admin\", \"users\".\"username\" AS \"username\" FROM \"foo\".\"users\" AS \"users\""]
```

Schema can also be renamed when necessary:

```clojure
(require '[com.verybigthings.penkala.env :as env])

(r/get-select-query (r/spec->relation users-spec-with-schema) (env/with-schema-rename "foo" "bar"))

=> ["SELECT \"users\".\"id\" AS \"id\", \"users\".\"is_admin\" AS \"is-admin\", \"users\".\"username\" AS \"username\" FROM \"bar\".\"users\" AS \"users\""]
```


### Aggregate functions

```clojure
(require '[com.verybigthings.penkala.helpers :as h])

;; avg

(-> users-rel
  (r/extend-with-aggregate :average [:avg :id])
  (r/get-select-query {}))

=> ["SELECT avg(\"users\".\"id\") AS \"average\", \"users\".\"id\" AS \"id\", \"users\".\"is_admin\" AS \"is-admin\", \"users\".\"username\" AS \"username\" FROM \"users\" AS \"users\" GROUP BY \"users\".\"id\", \"users\".\"is_admin\", \"users\".\"username\""]

;; max

(-> users-rel
  (r/extend-with-aggregate :maximum [:max :id])
  (r/get-select-query {}))

=> ["SELECT \"users\".\"id\" AS \"id\", \"users\".\"is_admin\" AS \"is-admin\", max(\"users\".\"id\") AS \"maximum\", \"users\".\"username\" AS \"username\" FROM \"users\" AS \"users\" GROUP BY \"users\".\"id\", \"users\".\"is_admin\", \"users\".\"username\""]

;; min

(-> users-rel
  (r/extend-with-aggregate :minimum [:min :id])
  (r/get-select-query {}))

=> ["SELECT \"users\".\"id\" AS \"id\", \"users\".\"is_admin\" AS \"is-admin\", min(\"users\".\"id\") AS \"minimum\", \"users\".\"username\" AS \"username\" FROM \"users\" AS \"users\" GROUP BY \"users\".\"id\", \"users\".\"is_admin\", \"users\".\"username\""]

;; count

(-> users-rel
  (r/extend-with-aggregate :count [:count (h/l 1)])
  (r/get-select-query {}))

=> ["SELECT count(1) AS \"count\", \"users\".\"id\" AS \"id\", \"users\".\"is_admin\" AS \"is-admin\", \"users\".\"username\" AS \"username\" FROM \"users\" AS \"users\" GROUP BY \"users\".\"id\", \"users\".\"is_admin\", \"users\".\"username\""]
```

### DISTINCT and DISTINCT ON clause

```clojure
(r/get-select-query (r/distinct users-rel) {})

["SELECT DISTINCT \"users\".\"id\" AS \"id\", \"users\".\"is_admin\" AS \"is-admin\", \"users\".\"username\" AS \"username\" FROM \"users\" AS \"users\""]
```

```clojure
(r/get-select-query (r/distinct users-rel [:id]) {})

=> ["SELECT DISTINCT ON(\"users\".\"id\") \"users\".\"id\" AS \"id\", \"users\".\"is_admin\" AS \"is-admin\", \"users\".\"username\" AS \"username\" FROM \"users\" AS \"users\""]
```

### EXCEPT operator

Creates a relation that is a combination of two relations with the EXCEPT operator.

EXCEPT example

```clojure
(-> users-rel
	(r/except (r/where users-rel [:is-true :is-admin]))
	(r/get-select-query {}))

=> ["SELECT \"users\".\"id\" AS \"id\", \"users\".\"is-admin\" AS \"is-admin\", \"users\".\"username\" AS \"username\" FROM (SELECT \"users\".\"id\" AS \"id\", \"users\".\"is_admin\" AS \"is-admin\", \"users\".\"username\" AS \"username\" FROM \"users\" AS \"users\" EXCEPT SELECT \"users\".\"id\" AS \"id\", \"users\".\"is_admin\" AS \"is-admin\", \"users\".\"username\" AS \"username\" FROM \"users\" AS \"users\" WHERE \"users\".\"is_admin\" IS TRUE) AS \"users\""]
```

### HAVING clause

```clojure
(-> users-rel
  (r/extend-with-aggregate :count [:count (h/l 1)])
  (r/having [:> :count (h/l 1)])
  (r/get-select-query {}))

=> ["SELECT count(1) AS \"count\", \"users\".\"id\" AS \"id\", \"users\".\"is_admin\" AS \"is-admin\", \"users\".\"username\" AS \"username\" FROM \"users\" AS \"users\" GROUP BY \"users\".\"id\", \"users\".\"is_admin\", \"users\".\"username\" HAVING count(1) > 1"]
```

### INTERSECT operator

```clojure
;; ignored by seancorfield/readme

(let [users (:users *env*)]
  (select! *env* (r/intersect users (r/where users [:is-true :is-admin]))))

=> [#:users{:username test@test.com, :is_admin false, :id 1}]
```

```clojure
(-> users-rel
	(r/intersect admins-rel)
	(r/get-select-query {}))

=> ["SELECT \"users\".\"id\" AS \"id\", \"users\".\"is-admin\" AS \"is-admin\", \"users\".\"username\" AS \"username\" FROM (SELECT \"users\".\"id\" AS \"id\", \"users\".\"is_admin\" AS \"is-admin\", \"users\".\"username\" AS \"username\" FROM \"users\" AS \"users\" INTERSECT SELECT \"users\".\"id\" AS \"id\", \"users\".\"is_admin\" AS \"is-admin\", \"users\".\"username\" AS \"username\" FROM \"users\" AS \"users\" WHERE \"users\".\"is_admin\" IS TRUE) AS \"users\""]
```

### JOIN clause

```clojure
(def posts-spec {:name "posts"
                 :columns ["id" "user_id" "body"]
                 :pk ["id"]})
  
(def posts-rel (r/spec->relation users-spec))

(r/get-select-query (r/join posts-rel :left (r/where users-rel [:is-true :is-admin]) :author [:= :user-id :author/id]) {})

=> ["SELECT \"users\".\"id\" AS \"id\", \"users\".\"is_admin\" AS \"is-admin\", \"users\".\"username\" AS \"username\", \"author\".\"id\" AS \"author__id\", \"author\".\"is-admin\" AS \"author__is-admin\", \"author\".\"username\" AS \"author__username\" FROM \"users\" AS \"users\" LEFT OUTER JOIN (SELECT \"users\".\"id\" AS \"id\", \"users\".\"is_admin\" AS \"is-admin\", \"users\".\"username\" AS \"username\" FROM \"users\" AS \"users\" WHERE \"users\".\"is_admin\" IS TRUE) \"author\" ON ? = \"author\".\"id\"" "user-id"]
```

### LIMIT clause

```clojure
(r/get-select-query (r/limit users-rel 10) {})

=> ["SELECT \"users\".\"id\" AS \"id\", \"users\".\"is_admin\" AS \"is-admin\", \"users\".\"username\" AS \"username\" FROM \"users\" AS \"users\" LIMIT 10"]
```

### LOCK command

```clojure
;; ignored by seancorfield/readme

(select! *env* (-> *env* :users (r/lock :share)))

=> [#:users{:is-admin false, :username test@test.com, :id 1} #:users{:is-admin true, :username admin@test.com, :id 2}]
```

```clojure
(r/get-select-query (r/lock users-rel :share) {})

=> ["SELECT \"users\".\"id\" AS \"id\", \"users\".\"is_admin\" AS \"is-admin\", \"users\".\"username\" AS \"username\" FROM \"users\" AS \"users\" FOR SHARE"]
```

### OFFSET clause

```clojure
(r/get-select-query (r/offset users-rel 10) {})

=> ["SELECT \"users\".\"id\" AS \"id\", \"users\".\"is_admin\" AS \"is-admin\", \"users\".\"username\" AS \"username\" FROM \"users\" AS \"users\" OFFSET 10"]
```

### ORDER BY clause

```clojure
(r/get-select-query (r/order-by users-rel [:id]) {})

=> ["SELECT \"users\".\"id\" AS \"id\", \"users\".\"is_admin\" AS \"is-admin\", \"users\".\"username\" AS \"username\" FROM \"users\" AS \"users\" ORDER BY \"users\".\"id\""]

(r/get-select-query (r/order-by users-rel [[:id :desc]]) {})

=> ["SELECT \"users\".\"id\" AS \"id\", \"users\".\"is_admin\" AS \"is-admin\", \"users\".\"username\" AS \"username\" FROM \"users\" AS \"users\" ORDER BY \"users\".\"id\" DESC"]

(r/get-select-query (r/order-by users-rel [[:id :desc][:username]]) {})

=> ["SELECT \"users\".\"id\" AS \"id\", \"users\".\"is_admin\" AS \"is-admin\", \"users\".\"username\" AS \"username\" FROM \"users\" AS \"users\" ORDER BY \"users\".\"id\" DESC, \"users\".\"username\""]

(r/get-select-query (r/order-by users-rel [[:id :desc :nulls-first]]) {})

=> ["SELECT \"users\".\"id\" AS \"id\", \"users\".\"is_admin\" AS \"is-admin\", \"users\".\"username\" AS \"username\" FROM \"users\" AS \"users\" ORDER BY \"users\".\"id\" DESC NULLS FIRST"]
```

### SELECT statement

```clojure
(r/get-select-query (r/select users-rel [:id, :username]) {})

=> ["SELECT \"users\".\"id\" AS \"id\", \"users\".\"username\" AS \"username\" FROM \"users\" AS \"users\""]
```

### UNION operator

```clojure
(r/get-select-query (r/union users-rel (r/where users-rel [:is-true :is-admin])) {})

=> ["SELECT \"users\".\"id\" AS \"id\", \"users\".\"is-admin\" AS \"is-admin\", \"users\".\"username\" AS \"username\" FROM (SELECT \"users\".\"id\" AS \"id\", \"users\".\"is_admin\" AS \"is-admin\", \"users\".\"username\" AS \"username\" FROM \"users\" AS \"users\" UNION SELECT \"users\".\"id\" AS \"id\", \"users\".\"is_admin\" AS \"is-admin\", \"users\".\"username\" AS \"username\" FROM \"users\" AS \"users\" WHERE \"users\".\"is_admin\" IS TRUE) AS \"users\""]
```

### UNION ALL operator

```clojure
(r/get-select-query (r/union-all users-rel (r/where users-rel [:is-true :is-admin])) {})

=> ["SELECT \"users\".\"id\" AS \"id\", \"users\".\"is-admin\" AS \"is-admin\", \"users\".\"username\" AS \"username\" FROM (SELECT \"users\".\"id\" AS \"id\", \"users\".\"is_admin\" AS \"is-admin\", \"users\".\"username\" AS \"username\" FROM \"users\" AS \"users\" UNION ALL SELECT \"users\".\"id\" AS \"id\", \"users\".\"is_admin\" AS \"is-admin\", \"users\".\"username\" AS \"username\" FROM \"users\" AS \"users\" WHERE \"users\".\"is_admin\" IS TRUE) AS \"users\""]
```

### WHERE clause

```clojure
(def admins-rel (r/where users-rel [:is-true :is-admin]))

(r/get-select-query admins-rel {})

=> ["SELECT \"users\".\"id\" AS \"id\", \"users\".\"is_admin\" AS \"is-admin\", \"users\".\"username\" AS \"username\" FROM \"users\" AS \"users\" WHERE \"users\".\"is_admin\" IS TRUE"]
```