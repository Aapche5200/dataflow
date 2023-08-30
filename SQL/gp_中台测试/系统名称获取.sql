SELECT
    relname AS table_name,
    obj_description(c.oid, 'pg_class') AS table_comment,
    pg_get_userbyid(c.relowner) AS table_owner
FROM  pg_class c
JOIN pg_roles r ON c.relowner = r.oid
WHERE
    relkind = 'r'
    AND relname LIKE '%ex_%'
ORDER BY
    table_name;