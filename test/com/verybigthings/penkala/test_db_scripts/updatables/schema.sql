CREATE SCHEMA public;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE normal_pk (
  id serial NOT NULL PRIMARY KEY,
  field1 text NOT NULL,
  field2 text,
  json_field jsonb,
  array_field text[],
  array_of_json json[]
);

CREATE TABLE uuid_pk (
  id uuid NOT NULL PRIMARY KEY DEFAULT gen_random_uuid (),
  field1 text
);

CREATE TABLE compound_pk (
  id1 serial NOT NULL,
  id2 serial NOT NULL,
  field1 text
);

ALTER TABLE compound_pk
  ADD PRIMARY KEY (id1, id2);

CREATE TABLE no_pk (
  field1 text,
  field2 text
);

CREATE TABLE "CasedName" (
  "Id" serial NOT NULL PRIMARY KEY,
  "Field1" text
);

CREATE VIEW normal_as AS
SELECT
  *
FROM
  normal_pk
WHERE
  field1 LIKE 'a%';

INSERT INTO normal_pk (field1)
  VALUES ('alpha'), ('beta'), ('gamma');

INSERT INTO compound_pk (field1)
  VALUES ('alpha'), ('beta'), ('gamma');

INSERT INTO no_pk (field1, field2)
  VALUES ('alpha', 'beta'), ('gamma', 'delta'), ('epsilon', 'zeta');

INSERT INTO "CasedName" ("Field1")
  VALUES ('Alpha'), ('Beta'), ('Gamma');

CREATE TABLE things (
  id serial PRIMARY KEY,
  stuff text,
  name text
);

CREATE UNIQUE INDEX stuff_name_idx ON things (stuff, lower(name));

CREATE TABLE booleans (
  id serial PRIMARY KEY,
  value boolean
);

CREATE TABLE booleans_with_default (
  id serial PRIMARY KEY,
  value boolean DEFAULT FALSE
);

CREATE TABLE things_2 (
  id serial PRIMARY KEY,
  my_stuff text,
  my_name text
);

CREATE UNIQUE INDEX my_stuff_my_name_idx ON things_2 (my_stuff, lower(my_name));

CREATE TABLE resources (
  id serial PRIMARY KEY,
  title text NOT NULL,
  deleted_at timestamp
);

CREATE UNIQUE INDEX resources_title_index ON resources (title);

