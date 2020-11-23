# Penkala

[![Clojars Project](https://img.shields.io/clojars/v/com.verybigthings/penkala.svg)](https://clojars.org/com.verybigthings/penkala)

Penkala is a composable query builder for PostgreSQL written in Clojure.

## Motivation

Clojure has a number of libraries to interact with the database layer. So, why write another one? For my needs, I find them
all operating on a wrong level of abstraction. Libraries like [HugSQL](https://www.hugsql.org/) and [HoneySQL](https://github.com/seancorfield/honeysql) allow you to use full extent of SQL, but 
still require you to write a lot of boilerplate, while libraries like [Walkable](https://github.com/walkable-server/walkable) and [seql](https://github.com/exoscale/seql) operate on a very high level, but are 
eschewing SQL and instead use EQL to write the queries.

I wanted something in the middle:

- Support for SQL semantics
- High(er) level API than HugSQL or HoneySQL have
- Implementation that is not "taking over" - you should be able to use Penkala in combination with any other library if you need to
- Composability
- API that is as pure as possible
- First class support for PostgreSQL features

The idea is to get as close to the features offered by ORM libraries in other languages, without taking on all the complexity
that ORMs bring with them.

## Relation as a first class citizen

The API is inspired by many libraries (Ecto, Massive.js, rasql), but the main inspiration comes from a library called bmg - written in Ruby. You can read more about its design decisions
 [here](http://www.try-alf.org/blog/2013-10-21-relations-as-first-class-citizen). While Penkala is _not_ implementing 
 relational algebra, and is instead staying close to the SQL semantics, the API is designed around the same principles - composition
 and reuse are built in.
 
Every operation on a relation is returning a _new_ relation that you can use and reuse in other queries.
 
## Usage

```clojure
(require '[com.verybigthings.penkala.relation :as r])

(def users-spec {:name "users"
                 :columns ["id" "username" "is_admin"]
                 :pk ["id"]
                 :schema "public"})

(def users-rel (r/spec->relation users-spec))

(r/get-select-query users-rel {})

=> ["SELECT \"users\".\"id\" AS \"id\", \"users\".\"is_admin\" AS \"is-admin\", \"users\".\"username\" AS \"username\" FROM \"public\".\"users\" AS \"users\""]
```

To create a relation, you need a relation spec. This describes the table (or view) columns, it's name, primary key and schema.
The spec can be used to create a relation, which can then be used to generate queries.

`r/get-select-query` function returns a "sqlvec" where the first element in the vector is the SQL query, and the remaining 
elements are the query parameters.

```clojure
(def admins-rel (r/where users-rel [:is-true :is-admin]))
(r/get-select-query admins-rel {})

=> ["SELECT \"users\".\"id\" AS \"id\", \"users\".\"is_admin\" AS \"is-admin\", \"users\".\"username\" AS \"username\" FROM \"public\".\"users\" AS \"users\" WHERE \"users\".\"is_admin\" IS TRUE"]
```

In this case, we've created a new relation - `admins-rel` which is representing only the admin users.

Let's introduce a new relation - `posts` and load posts only written by admins

```clojure
(def posts-spec {:name "posts"
                 :columns ["id" "user_id" "body"]
                 :pk ["id"]
                 :schema "public"})
  
(def posts-rel (r/spec->relation users-spec))

(r/get-select-query (r/where posts-rel [:in :user-id (r/select admins-rel [:id])]) {})
=> ["SELECT \"posts\".\"body\" AS \"body\", \"posts\".\"id\" AS \"id\", \"posts\".\"user_id\" AS \"user-id\" FROM \"public\".\"posts\" AS \"posts\" WHERE \"posts\".\"user_id\" IN (SELECT \"sq_7758__users\".\"id\" AS \"id\" FROM \"public\".\"users\" AS \"sq_7758__users\" WHERE \"sq_7758__users\".\"is_admin\" IS TRUE)"]
```

Or maybe, we want only posts posted by admins joined with their author:

```clojure
(r/get-select-query (r/join posts-rel :left admins-rel :author [:= :user-id :author/id]) {})
=> ["SELECT \"posts\".\"body\" AS \"body\", \"posts\".\"id\" AS \"id\", \"posts\".\"user_id\" AS \"user-id\", \"author\".\"id\" AS \"author__id\", \"author\".\"is-admin\" AS \"author__is-admin\", \"author\".\"username\" AS \"author__username\" FROM \"public\".\"posts\" AS \"posts\" LEFT OUTER JOIN (SELECT \"users\".\"id\" AS \"id\", \"users\".\"is_admin\" AS \"is-admin\", \"users\".\"username\" AS \"username\" FROM \"public\".\"users\" AS \"users\" WHERE \"users\".\"is_admin\" IS TRUE) \"author\" ON \"posts\".\"user_id\" = \"author\".\"id\""]
```

_Notice that you're using namespaced keywords when you're referencing columns in joined relations, where the namespace is the
alias used to join the relation._

As you can see, Penkala takes care of all the nitty-gritty details around sub-selects, joins and aliasing. Relation API
functions are implemented, documented and spec'ed in the `com.verybigthings.penkala.relation` namespace.

Penkala provides the integration with the [next.jdbc]() library, which will query the DB, get the information about the
tables and views and return a map with relations based on this information.

```clojure
(require '[com.verybigthings.penkala.next-jdbc :refer [get-env]])
(get-env "db-url-or-spec")
```


### Value expressions

A lot of the relation API functions accept value expressions. Value expressions are used to write where clauses, extend 
relations with computed columns, write having clauses, etc.. Value expressions API is heavily inspired by the HoneySQ library,
and value expressions are written as vectors:

```clojure
[:= :id 1]
```

In this case Penkala will look for a column named `:id` and compare it to the value `1`. Value expressions can be arbitrarly
nested to any depth, and allow you to write almost any SQL you might need.

Some examples:

```clojure
;; Complex where predicate
(-> *env* :products (r/where [:and [:= :id 1] [:= :price 12.0] [:is-null :tags]]))

;; Querying a JSON field
(-> *env* :products (r/where [:= ["#>>" :specs ["weight"]] [:cast 30 "text"]]))

;; Use a POSIX regex in a where clause
(-> *env* :products (r/where ["!~*" :name "product[ ]*[2-4](?!otherstuff)"]))

;; Use the "overlap" operator
(-> *env* :products (r/where ["&&" :tags ["tag3" "tag4" "tag5"]]))
```

When writing a value expression, first element of a vector will be used to determine the type of the expression (operator, function, etc..) and the
rest will be sent as arguments. Keywords have a special meaning in value expressions, and when encountering them (anywhere except in the first position), 
Penkala will check if there is a column with that name in the current relation. If there is, it will treat it as a column name, and if it is not, it will
treat it as a value. If you need to explicitly treat something as a value or param or column... `com.verybigthings.penkala.helpers` provides wrapper 
functions which will allow you to explicitly mark something as a value or as a column.

One of the cases where you'll need to explicitly wrap keywords is when you want to provide named params to the query. So, far all examples
inlined the params in the value expression, but Penkala provides a better way, which will allow you to write reusable value expressions:

```clojure
(require '[com.verybigthings.penkala.helpers :refer [param]])
(r/get-select-query (r/where posts-rel [:= :user-id (param :user/id )]) {} {:user/id 1})
=> ["SELECT \"posts\".\"body\" AS \"body\", \"posts\".\"id\" AS \"id\", \"posts\".\"user_id\" AS \"user-id\" FROM \"public\".\"posts\" AS \"posts\" WHERE \"posts\".\"user_id\" = ?" 1]
```

### Decomposition

If the first half of boilerplate you have to write when you generate SQL queries is composition, then the second one is _decomposition_
of results into a graph form (nested maps and vectors). Penkala provides a decomposition layer which allows you to get the results
in the format that's usable. Penkala can infer the decomposition schema from the relation, so in most cases you won't have to do
anything to get it working. In other cases, you can override the behavior. This allows you to query the DB with a convenience of an ORM
without having to use limited concepts like has many, has one, many to many, you get the same results while being able to use the rest of
the Penkala API.

Here's an example from the test suite:

```clojure
(ns com.verybigthings.penkala.relation.join-test
  (:require [clojure.test :refer :all]
            [com.verybigthings.penkala.next-jdbc :refer [select!]]
            [com.verybigthings.penkala.relation :as r]
            [com.verybigthings.penkala.test-helpers :as th :refer [*env*]]
            [com.verybigthings.penkala.decomposition :refer [map->DecompositionSchema]]))

(deftest it-joins-multiple-tables-at-multiple-levels
  (let [alpha (:alpha *env*)
        beta (:beta *env*)
        gamma (:gamma *env*)
        sch-delta (:sch/delta *env*)
        sch-epsilon (:sch/epsilon *env*)
        joined (-> alpha
                 (r/join :inner
                   (-> beta
                     (r/join :inner gamma :gamma [:= :id :gamma/beta-id])
                     (r/join :inner sch-delta :delta [:= :id :delta/beta-id]))
                   :beta
                   [:= :id :beta/alpha-id])
                 (r/join :inner sch-epsilon :epsilon [:= :id :epsilon/alpha-id]))
        res (select! *env* joined)]
    (is (= [#:alpha{:val "one"
                    :id 1
                    :beta
                    [#:beta{:val "alpha one"
                            :j nil
                            :id 1
                            :alpha-id 1
                            :gamma
                            [#:gamma{:beta-id 1
                                     :alpha-id-one 1
                                     :alpha-id-two 1
                                     :val "alpha one alpha one beta one"
                                     :j nil
                                     :id 1}]
                            :delta
                            [#:delta{:beta-id 1 :val "beta one" :id 1}]}],
                    :epsilon [#:epsilon{:val "alpha one" :id 1 :alpha-id 1}]}]
          res))))
```

In this case, multiple relations were joined (on multiple levels) and then transformed into a graph form. Decomposition schema
was inferred from the relation joins structure.

## Credits

I've spent a lot of time researching other libraries that are doing the similar thing, but two of them affected Penkala the most

- [Massive.js](https://massivejs.org/) - Penkala started as a port of this lib, and I'm thankful I was able to study its source code to learn how to approach the architecture. Also, the tests and SQL queries to get the DB structure are copied from the Massive.js codebase.
- [bmg](https://github.com/enspirit/bmg) - this library helped me to design the API in a way that's composable and reusable

Next.jdbc integration code is taken from Luminus, so I want to thank the Lumius team for that effort.

Other libraries I've researched:

- https://github.com/cdinger/rasql
- https://github.com/active-group/sqlosure
- https://clojureql.sabrecms.com/en/welcome
- https://github.com/tatut/specql

Penkala development is kindly sponsored by [Very Big Things](https://verybigthings.com)

## License

Copyright © 2020 Mihael Konjevic (https://verybigthings.com)

Distributed under the MIT License.
