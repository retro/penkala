# Querying

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

## Examples

### where

Adding WHERE clause:

```clojure
(def admins-rel (r/where users-rel [:is-true :is-admin]))

(r/get-select-query admins-rel {})

=> ["SELECT \"users\".\"id\" AS \"id\", \"users\".\"is_admin\" AS \"is-admin\", \"users\".\"username\" AS \"username\" FROM \"users\" AS \"users\" WHERE \"users\".\"is_admin\" IS TRUE"]
```

### distinct

Adding a DISTINCT and DISTINCT ON clause:

```clojure
(r/get-select-query (r/distinct users-rel) {})

["SELECT DISTINCT \"users\".\"id\" AS \"id\", \"users\".\"is_admin\" AS \"is-admin\", \"users\".\"username\" AS \"username\" FROM \"users\" AS \"users\""]
```

```clojure
(r/get-select-query (r/distinct users-rel [:id]) {})

=> ["SELECT DISTINCT ON(\"users\".\"id\") \"users\".\"id\" AS \"id\", \"users\".\"is_admin\" AS \"is-admin\", \"users\".\"username\" AS \"username\" FROM \"users\" AS \"users\""]
```

### except

Creates a relation that is a combination of two relations with the EXCEPT operator.

EXCEPT example

```clojure
(-> users-rel
	(r/except admins-rel)
	(r/get-select-query {}))

=> ["SELECT \"users\".\"id\" AS \"id\", \"users\".\"is-admin\" AS \"is-admin\", \"users\".\"username\" AS \"username\" FROM (SELECT \"users\".\"id\" AS \"id\", \"users\".\"is_admin\" AS \"is-admin\", \"users\".\"username\" AS \"username\" FROM \"users\" AS \"users\" EXCEPT SELECT \"users\".\"id\" AS \"id\", \"users\".\"is_admin\" AS \"is-admin\", \"users\".\"username\" AS \"username\" FROM \"users\" AS \"users\" WHERE \"users\".\"is_admin\" IS TRUE) AS \"users\""]
```

### extend-with-aggregate

COUNT example:

```clojure
(require '[com.verybigthings.penkala.helpers :as h])

(-> users-rel
  (r/extend-with-aggregate :count [:count (h/l 1)])
  (r/get-select-query {}))

=> ["SELECT count(1) AS \"count\", \"users\".\"id\" AS \"id\", \"users\".\"is_admin\" AS \"is-admin\", \"users\".\"username\" AS \"username\" FROM \"users\" AS \"users\" GROUP BY \"users\".\"id\", \"users\".\"is_admin\", \"users\".\"username\""]
```

### having

And having operation. If there's already a having clause set, this clause will be joined with AND.

```clojure
(-> users-rel
  (r/extend-with-aggregate :count [:count (h/l 1)])
  (r/having [:> :count (h/l 1)])
  (r/get-select-query {}))

=> ["SELECT count(1) AS \"count\", \"users\".\"id\" AS \"id\", \"users\".\"is_admin\" AS \"is-admin\", \"users\".\"username\" AS \"username\" FROM \"users\" AS \"users\" GROUP BY \"users\".\"id\", \"users\".\"is_admin\", \"users\".\"username\" HAVING count(1) > 1"]
```

### intersect

Creates a relation that is a combination of two relations with the INTERSECT operator.

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

### join

```clojure
(def posts-spec {:name "posts"
                 :columns ["id" "user_id" "body"]
                 :pk ["id"]})
  
(def posts-rel (r/spec->relation users-spec))

(r/get-select-query (r/join posts-rel :left admins-rel :author [:= :user-id :author/id]) {})
```

### limit

```clojure
(r/get-select-query (r/limit users-rel 10) {})
```

### lock

Locks selected rows.

```clojure
;; ignored by seancorfield/readme

(select! *env* (-> *env* :users (r/lock :share)))

=> [#:users{:is-admin false, :username test@test.com, :id 1} #:users{:is-admin true, :username admin@test.com, :id 2}]
```

```clojure
(r/get-select-query (r/lock users-rel :share) {})

=> ["SELECT \"users\".\"id\" AS \"id\", \"users\".\"is_admin\" AS \"is-admin\", \"users\".\"username\" AS \"username\" FROM \"users\" AS \"users\" FOR SHARE"]
```

### offset

```clojure
(r/get-select-query (r/offset users-rel 10) {})
```

### order-by

```clojure
(r/get-select-query (r/order-by users-rel [:id]) {})
(r/get-select-query (r/order-by users-rel [[:id :desc]]) {})
(r/get-select-query (r/order-by users-rel [[:id :desc :nulls-first]]) {})
```

### select

```clojure
(r/get-select-query (r/select users-rel [:id, :username]) {})
```

### union

```clojure
(r/get-select-query (r/union users-rel admins-rel) {})
```

### union-all

```clojure
(r/get-select-query (r/union-all users-rel admins-rel) {})
```
