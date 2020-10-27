-- :name create-document-table
/* :require [hugsql.parameters :refer [identifier-param-quote]] */
CREATE TABLE :i:schema.:i:table(
  id :sql:pk-type PRIMARY KEY :sql:pk-default,
  body jsonb NOT NULL,
  search tsvector,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz
);

CREATE INDEX idx_:sql:index ON :i:schema.:i:table USING GIN(body jsonb_path_ops);
CREATE INDEX idx_search_:sql:index ON :i:schema.:i:table USING GIN(search);

CREATE OR REPLACE FUNCTION penkala_document_inserted()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY definer
AS $$
BEGIN
  NEW.search = to_tsvector(NEW.body::text);
  RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION penkala_document_updated()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY definer
AS $$
BEGIN
  NEW.updated_at = now();
  NEW.search = to_tsvector(NEW.body::text);
  RETURN NEW;
END;
$$;

CREATE TRIGGER
--~ (str (:schema params) "_" (:table params) "_inserted")
BEFORE INSERT ON :i:schema.:i:table
FOR EACH ROW EXECUTE PROCEDURE penkala_document_inserted();

CREATE TRIGGER
--~ (str (:schema params) "_" (:table params) "_updated")
BEFORE UPDATE ON :i:schema.:i:table
FOR EACH ROW EXECUTE PROCEDURE penkala_document_updated();

COMMENT ON TABLE
--~ (str (identifier-param-quote (:schema params) options) "." (identifier-param-quote (:table params) options))
IS 'A document table generated with com.verybigthings/penkala.';
COMMENT ON COLUMN
--~ (str (identifier-param-quote (:schema params) options) "." (identifier-param-quote (:table params) options) ".id")
IS 'The document primary key. Will be added to the body when retrieved using penkala document functions';
COMMENT ON COLUMN
--~ (str (identifier-param-quote (:schema params) options) "." (identifier-param-quote (:table params) options) ".body")
IS 'The document body, stored without primary key.';
COMMENT ON COLUMN
--~ (str (identifier-param-quote (:schema params) options) "." (identifier-param-quote (:table params) options) ".search")
IS 'Search vector for full-text search support.';
COMMENT ON COLUMN
--~ (str (identifier-param-quote (:schema params) options) "." (identifier-param-quote (:table params) options) ".created_at")
IS 'Timestamp for document creation.';
COMMENT ON COLUMN
--~ (str (identifier-param-quote (:schema params) options) "." (identifier-param-quote (:table params) options) ".updated_at")
IS 'Timestamp for the record''s last modification.';
