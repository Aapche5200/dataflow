--# 获取所有表名及注释
SELECT
    relname AS table_name,
    obj_description(c.oid, 'pg_class') AS table_comment,
    pg_get_userbyid(c.relowner) AS table_owner
FROM  pg_class c
JOIN pg_roles r ON c.relowner = r.oid
WHERE
    relkind = 'r'
    AND relname NOT LIKE 'pg_%'
    AND relname NOT LIKE 'information_%'
ORDER BY
    table_name;


--# 获取所有表名及注释
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


-- 获取字段名、类型、注释、是否为空
SELECT a.attname                             as column_name,
       col_description(a.attrelid, a.attnum) as column_comment,
       format_type(a.atttypid, a.atttypmod)  as data_type
       --a.attnotnull                          as notnull
FROM pg_class as c,
     pg_attribute as a
where c.relname = 'dwd_paas_uf_product_infov2'
  and a.attrelid = c.oid
  and a.attnum > 0;



SELECT relname                            AS table_name,
       obj_description(c.oid, 'pg_class') AS table_comment,
       coalesce(job_owner,pg_get_userbyid(c.relowner))       AS table_owner
FROM pg_class c
         JOIN pg_roles r ON c.relowner = r.oid
left join ods_gp_job_schedule_pool as p on c.relname=substr(p.job_name,6,length(p.job_name)-5)
WHERE relkind = 'r'
  AND relname NOT LIKE 'pg_%%'
  AND relname NOT LIKE 'information_%%'

