-- :name get-functions
-- :result :many
--
-- Load non-system functions. The allowed and forbidden are overlapped such
-- that more specific forbidden entries may override more general allowed
-- entries.
--
-- Parameters:
-- function/allowed: array or comma-delimited string of LIKE conditions.
-- function/forbidden: array or comma-delimited string of LIKE conditions.

SELECT * FROM (
  SELECT DISTINCT
    n.nspname AS schema,
    (NOT p.proretset) AS "singleRow",
    (t.typtype IN ('b', 'd', 'e', 'r')) AS "singleValue",
    p.proname AS name,
    p.prokind AS kind,
    p.provariadic AS "isVariadic"
  FROM pg_proc p
  JOIN pg_namespace n ON p.pronamespace = n.oid
  JOIN pg_type t on p.prorettype = t.oid
  WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
) fns
WHERE CASE
  WHEN :functions/allowed <> '' THEN
    -- allowed specific functions, with fully-qualified name (no schema assumes public).
    replace((fns.schema || '.'|| fns.name), 'public.', '') LIKE ANY(string_to_array(replace(:functions/allowed, ' ', ''), ','))
  WHEN :schemas/allowed <> '' OR :functions/forbidden <> '' THEN ((
    :schemas/allowed = ''
    OR
    -- allow specific schemas (none or '' assumes all):
    schema = ANY(string_to_array(replace(:schemas/allowed, ' ', ''), ','))
  ) AND (
    :functions/forbidden = ''
    OR
    -- forbidden tables using LIKE by fully-qualified name (no schema assumes public):
    replace((schema || '.'|| name), 'public.', '') NOT LIKE ALL(string_to_array(replace(:functions/forbidden, ' ', ''), ','))
  )) OR
    -- make exceptions for specific functions, with fully-qualified name or wildcard pattern (no schema assumes public).
    replace((schema || '.'|| name), 'public.', '') LIKE ANY(string_to_array(replace(:functions/exceptions, ' ', ''), ','))
  ELSE TRUE
END
ORDER BY schema, name;
