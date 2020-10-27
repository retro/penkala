-- :name get-tables
-- :result :many
--
-- Load tables.
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
  WITH table_columns AS (
    SELECT attrelid, array_agg(DISTINCT attname::text) AS columns
    FROM pg_catalog.pg_attribute
    WHERE attnum > 0 AND attisdropped IS FALSE
    GROUP BY attrelid
  ), table_primary_keys AS (
    SELECT c.conrelid, array_agg(att.attname::text) AS columns
    FROM pg_catalog.pg_constraint AS c
    JOIN pg_catalog.pg_attribute AS att
      ON att.attrelid = c.conrelid
      AND att.attnum = ANY (c.conkey)
    WHERE c.contype = 'p'
    GROUP BY c.conrelid
  ), foreign_keys AS (
    SELECT foreign_keys.conname AS constraint_name,
      originrel.relname AS origin_name,
      originns.nspname AS origin_schema,
      array_agg(DISTINCT originatt.attname::text) AS origin_columns,
      dependentrel.relname AS dependent_name,
      dependentns.nspname AS dependent_schema,
      array_agg(DISTINCT dependentatt.attname::text) AS dependent_columns
    FROM pg_catalog.pg_constraint AS foreign_keys
      JOIN pg_catalog.pg_class AS originrel ON originrel.oid = foreign_keys.confrelid
      JOIN pg_catalog.pg_namespace AS originns ON originns.oid = originrel.relnamespace
      JOIN pg_catalog.pg_attribute AS originatt ON originatt.attrelid = originrel.oid AND originatt.attnum = ANY(foreign_keys.confkey)
      JOIN pg_catalog.pg_class AS dependentrel ON dependentrel.oid = foreign_keys.conrelid
      JOIN pg_catalog.pg_namespace AS dependentns ON dependentns.oid = dependentrel.relnamespace
      JOIN pg_catalog.pg_attribute AS dependentatt ON dependentatt.attrelid = dependentrel.oid AND dependentatt.attnum = ANY(foreign_keys.conkey)
    WHERE foreign_keys.contype = 'f'
    GROUP BY foreign_keys.conname, originrel.relname, originns.nspname, dependentrel.relname, dependentns.nspname
  )
  SELECT t.table_schema AS schema,
      t.table_name AS name,
      parent.relname AS parent,
      pks.columns AS pk,
      fks.constraint_name AS fk,
      fks.dependent_columns AS fk_dependent_columns,
      fks.origin_schema AS fk_origin_schema,
      fks.origin_name AS fk_origin_name,
      fks.origin_columns AS fk_origin_columns,
      c.columns,
      TRUE AS is_insertable_into
    FROM information_schema.tables t
    JOIN pg_catalog.pg_namespace nsp
      ON nsp.nspname = t.table_schema
    JOIN pg_catalog.pg_class cls
      ON cls.relnamespace = nsp.oid
      AND cls.relname = t.table_name
    LEFT OUTER JOIN table_primary_keys pks
      ON pks.conrelid = cls.oid
    LEFT OUTER JOIN foreign_keys fks
      ON fks.dependent_schema = t.table_schema
      AND fks.dependent_name = t.table_name
    JOIN table_columns AS c
      ON c.attrelid = cls.oid

    -- get parent table if there is one
    LEFT OUTER JOIN pg_catalog.pg_inherits inh ON inh.inhrelid = cls.oid
    LEFT OUTER JOIN pg_catalog.pg_class AS parent ON inh.inhparent = parent.oid
    LEFT OUTER JOIN pg_catalog.pg_namespace AS parentschema
      ON parentschema.oid = parent.relnamespace
    WHERE t.table_schema NOT IN ('information_schema', 'pg_catalog')
      AND t.table_type NOT IN ('VIEW', 'FOREIGN TABLE')

  UNION

  SELECT t.table_schema AS schema,
    t.table_name AS name,
    NULL AS parent,
    NULL AS pk,
    NULL AS fk,
    NULL AS fk_dependent_columns,
    NULL AS fk_origin_schema,
    NULL AS fk_origin_name,
    NULL AS fk_origin_columns,
    array_agg(c.attname::text) AS columns,
    CASE t.is_insertable_into WHEN 'YES' THEN TRUE ELSE FALSE END AS is_insertable_into
  FROM information_schema.tables t
  JOIN pg_catalog.pg_namespace nsp
    ON nsp.nspname = t.table_schema
  JOIN pg_catalog.pg_class cls
    ON cls.relnamespace = nsp.oid
    AND cls.relname = t.table_name
  JOIN pg_catalog.pg_attribute c
    ON c.attrelid = cls.oid
    AND c.attnum > 0
  WHERE t.table_type = 'FOREIGN TABLE'
  GROUP BY t.table_schema, t.table_name, t.is_insertable_into
) tables
WHERE CASE
  WHEN :relations/allowed <> '' THEN
    -- allowed specific tables, with fully-qualified name (no schema assumes public).
    replace((tables.schema || '.'|| tables.name), 'public.', '') LIKE ANY(string_to_array(replace(:relations/allowed, ' ', ''), ','))
  WHEN :schemas/allowed <> '' OR :relations/forbidden <> '' THEN ((
    :schemas/allowed = ''
    OR
    -- allow specific schemas (none or '' assumes all):
    schema = ANY(string_to_array(replace(:schemas/allowed, ' ', ''), ','))
  ) AND (
    :relations/forbidden = ''
    OR
    -- forbidden tables using LIKE by fully-qualified name (no schema assumes public):
    replace((schema || '.'|| name), 'public.', '') NOT LIKE ALL(string_to_array(replace(:relations/forbidden, ' ', ''), ','))
  )) OR
    -- make exceptions for specific tables, with fully-qualified name or wildcard pattern (no schema assumes public).
    replace((schema || '.'|| name), 'public.', '') LIKE ANY(string_to_array(replace(:relations/exceptions, ' ', ''), ','))
  ELSE TRUE
END;
