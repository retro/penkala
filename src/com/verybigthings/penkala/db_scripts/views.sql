-- :name get-views
-- :result :many
--
-- Load views and materialized views.
--
-- Parameters:
-- allowed: array or comma-delimited string of LIKE conditions applied to
--   tables. If specified, overrides all other parameters.
-- allowed-schemas: array or comma-delimited string of LIKE conditions applied
--   to schemas.
-- forbidden: array or comma-delimited string of LIKE conditions applied
--   negatively to tables.
-- exceptions: array or comma-delimited string of LIKE conditions which
--   override forbidden tables.

SELECT * FROM (
  SELECT
    v.schemaname AS schema,
    v.viewname AS name,
    array_agg(DISTINCT c.attname::text) AS columns,
    pg_relation_is_updatable(cls.oid::regclass, true) & 8 >= 8 AS is_insertable_into,
    FALSE AS is_matview
  FROM pg_views v
  JOIN pg_catalog.pg_namespace nsp
    ON nsp.nspname = v.schemaname
  JOIN pg_catalog.pg_class cls
    ON cls.relnamespace = nsp.oid
    AND cls.relname = v.viewname
  JOIN pg_catalog.pg_attribute c
    ON c.attrelid = cls.oid
    AND c.attnum > 0
  WHERE schemaname <> 'pg_catalog' AND schemaname <> 'information_schema'
  GROUP BY v.schemaname, v.viewname, cls.oid

  UNION

  SELECT
    schemaname AS schema,
    matviewname AS name,
    array_agg(DISTINCT c.attname::text) AS columns,
    FALSE AS is_insertable_into,
    TRUE AS is_matview
  FROM pg_matviews v
  JOIN pg_catalog.pg_namespace nsp
    ON nsp.nspname = v.schemaname
  JOIN pg_catalog.pg_class cls
    ON cls.relnamespace = nsp.oid
    AND cls.relname = v.matviewname
  JOIN pg_catalog.pg_attribute c
    ON c.attrelid = cls.oid
    -- ordinary columns have positive attnum
    AND c.attnum > 0
  GROUP BY v.schemaname, v.matviewname
) views
WHERE CASE
  WHEN :relations/allowed <> '' THEN
    -- allowed specific views, with fully-qualified name (no schema assumes public).
    replace((views.schema || '.'|| views.name), 'public.', '') LIKE ANY(string_to_array(replace(:relations/allowed, ' ', ''), ','))
  WHEN :schemas/allowed <> '' OR :relations/forbidden <> '' THEN ((
    :schemas/allowed = ''
    OR
    -- allow specific schemas (none or '' assumes all):
    schema = ANY(string_to_array(replace(:schemas/allowed, ' ', ''), ','))
  ) AND (
    :relations/forbidden = ''
    OR
    -- forbidden views using LIKE by fully-qualified name (no schema assumes public):
    replace((schema || '.'|| name), 'public.', '') NOT LIKE ALL(string_to_array(replace(:relations/forbidden, ' ', ''), ','))
  )) OR
    -- make exceptions for specific views, with fully-qualified name or wildcard pattern (no schema assumes public).
    replace((schema || '.'|| name), 'public.', '') LIKE ANY(string_to_array(replace(:relations/exceptions, ' ', ''), ','))
  ELSE TRUE
END;
