# Environment

To use Penkala in you project you will need to set up the ```environment```. First we need to prepare the foundation so let's create the database:

```sql
create database penkala_dev;
```

We'll also add a simple ```users``` table (don't forget to connect to the database ```\c penkala_dev```):

```sql
create table users(
  id serial primary key,
  username varchar(50) not null,
  is_admin boolean
);
```

Now we can use ```com.verybigthings.penkala.next-jdbc/get-env``` to get the environment information from the database:

```clojure
(require '[com.verybigthings.penkala.next-jdbc :refer [get-env]])

;; pay attention to DB creds
(def db-uri "jdbc:postgresql://localhost:5432/penkala_dev?user=user&password=password")

(def ^:dynamic *env* (get-env db-uri))

*env*

=> {:com.verybigthings.penkala.env/schema "public", :com.verybigthings.penkala.env/db "jdbc:postgresql://localhost:5432/penkala_dev?user=davor&password=davor", :users #com.verybigthings.penkala.relation.Relation{:spec {:schema "public", :fk-origin-name nil, :pk ["id"], :parent nil, :columns ["id" "is_admin" "username"], :name "users", :fk-origin-columns nil, :is-insertable-into true, :fk-origin-schema nil, :fk-dependent-columns nil, :fk nil, :namespace "users"}, :columns {:column-6763 {:type :concrete, :name "id"}, :column-6764 {:type :concrete, :name "is_admin"}, :column-6765 {:type :concrete, :name "username"}}, :ids->aliases {:column-6763 :id, :column-6764 :is-admin, :column-6765 :username}, :aliases->ids {:id :column-6763, :is-admin :column-6764, :username :column-6765}, :projection #{:is-admin :username :id}, :pk [:column-6763]}}
```

As can be seen, Penkala listed all tables (and views) and returned a map where keys are table names and values are relations.


## What about non-default schemas?

Let's extend our database with two schemas and create ```posts``` table in each of them:

```sql
create schema tenant1;

create schema tenant2;

create table tenant1.posts(
  id serial primary key,
  user_id int,
  body text
);

create table tenant2.posts(
  id serial primary key,
  user_id int,
  body text
);
```

Let's see how will this affect the environment:

```clojure
(def ^:dynamic *env* (get-env db-uri))

*env*

=> {:com.verybigthings.penkala.env/schema "public", :com.verybigthings.penkala.env/db "jdbc:postgresql://localhost:5432/penkala_dev?user=davor&password=davor", :tenant-2/posts #com.verybigthings.penkala.relation.Relation{:spec {:schema "tenant2", :fk-origin-name nil, :pk ["id"], :parent nil, :columns ["body" "id" "user_id"], :name "posts", :fk-origin-columns nil, :is-insertable-into true, :fk-origin-schema nil, :fk-dependent-columns nil, :fk nil, :namespace "posts"}, :columns {:column-6766 {:type :concrete, :name "body"}, :column-6767 {:type :concrete, :name "id"}, :column-6768 {:type :concrete, :name "user_id"}}, :ids->aliases {:column-6766 :body, :column-6767 :id, :column-6768 :user-id}, :aliases->ids {:body :column-6766, :id :column-6767, :user-id :column-6768}, :projection #{:user-id :id :body}, :pk [:column-6767]}, :tenant-1/posts #com.verybigthings.penkala.relation.Relation{:spec {:schema "tenant1", :fk-origin-name nil, :pk ["id"], :parent nil, :columns ["body" "id" "user_id"], :name "posts", :fk-origin-columns nil, :is-insertable-into true, :fk-origin-schema nil, :fk-dependent-columns nil, :fk nil, :namespace "posts"}, :columns {:column-6769 {:type :concrete, :name "body"}, :column-6770 {:type :concrete, :name "id"}, :column-6771 {:type :concrete, :name "user_id"}}, :ids->aliases {:column-6769 :body, :column-6770 :id, :column-6771 :user-id}, :aliases->ids {:body :column-6769, :id :column-6770, :user-id :column-6771}, :projection #{:user-id :id :body}, :pk [:column-6770]}, :users #com.verybigthings.penkala.relation.Relation{:spec {:schema "public", :fk-origin-name nil, :pk ["id"], :parent nil, :columns ["id" "is_admin" "username"], :name "users", :fk-origin-columns nil, :is-insertable-into true, :fk-origin-schema nil, :fk-dependent-columns nil, :fk nil, :namespace "users"}, :columns {:column-6772 {:type :concrete, :name "id"}, :column-6773 {:type :concrete, :name "is_admin"}, :column-6774 {:type :concrete, :name "username"}}, :ids->aliases {:column-6772 :id, :column-6773 :is-admin, :column-6774 :username}, :aliases->ids {:id :column-6772, :is-admin :column-6773, :username :column-6774}, :projection #{:is-admin :username :id}, :pk [:column-6772]}}
```

As can be seen, namespaced keys are used and namespace is the schema name.


## Fetching relations

```clojure
(:users *env*)

=> #com.verybigthings.penkala.relation.Relation{:spec {:schema "public", :fk-origin-name nil, :pk ["id"], :parent nil, :columns ["id" "is_admin" "username"], :name "users", :fk-origin-columns nil, :is-insertable-into true, :fk-origin-schema nil, :fk-dependent-columns nil, :fk nil, :namespace "users"}, :columns {:column-6781 {:type :concrete, :name "id"}, :column-6782 {:type :concrete, :name "is_admin"}, :column-6783 {:type :concrete, :name "username"}}, :ids->aliases {:column-6781 :id, :column-6782 :is-admin, :column-6783 :username}, :aliases->ids {:id :column-6781, :is-admin :column-6782, :username :column-6783}, :projection #{:is-admin :username :id}, :pk [:column-6781]}

(:tenant-1/posts *env*)

=> #com.verybigthings.penkala.relation.Relation{:spec {:schema "tenant1", :fk-origin-name nil, :pk ["id"], :parent nil, :columns ["body" "id" "user_id"], :name "posts", :fk-origin-columns nil, :is-insertable-into true, :fk-origin-schema nil, :fk-dependent-columns nil, :fk nil, :namespace "posts"}, :columns {:column-6807 {:type :concrete, :name "body"}, :column-6808 {:type :concrete, :name "id"}, :column-6809 {:type :concrete, :name "user_id"}}, :ids->aliases {:column-6807 :body, :column-6808 :id, :column-6809 :user-id}, :aliases->ids {:body :column-6807, :id :column-6808, :user-id :column-6809}, :projection #{:user-id :id :body}, :pk [:column-6808]}

(:tenant-2/posts *env*)

=> #com.verybigthings.penkala.relation.Relation{:spec {:schema "tenant2", :fk-origin-name nil, :pk ["id"], :parent nil, :columns ["body" "id" "user_id"], :name "posts", :fk-origin-columns nil, :is-insertable-into true, :fk-origin-schema nil, :fk-dependent-columns nil, :fk nil, :namespace "posts"}, :columns {:column-6804 {:type :concrete, :name "body"}, :column-6805 {:type :concrete, :name "id"}, :column-6806 {:type :concrete, :name "user_id"}}, :ids->aliases {:column-6804 :body, :column-6805 :id, :column-6806 :user-id}, :aliases->ids {:body :column-6804, :id :column-6805, :user-id :column-6806}, :projection #{:user-id :id :body}, :pk [:column-6805]}
```