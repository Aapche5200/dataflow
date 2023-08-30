--查看节点分布情况
select gp_segment_id,count(1) from person_info group by 1


--计算倾斜率
SELECT schemaname,
       tablename,
       attname,
       abs((pg_stats.correlation * sqrt(pg_stats.n_distinct)) /
           nullif(sqrt(stddev_samp((pg_stats.attname)::float8) OVER()), 0)) AS correlation_ratio
FROM pg_stats
         INNER JOIN pg_class ON pg_stats.tablename = pg_class.relname
         INNER JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace
WHERE pg_namespace.nspname NOT LIKE 'pg_%'
  AND pg_namespace.nspname != 'information_schema'
  AND pg_class.relkind = 'r'
  AND pg_stats.correlation IS NOT NULL
  AND pg_stats.n_distinct > 1
ORDER BY correlation_ratio DESC;




--1查看分区数据量分布情况：
SELECT partition_column, COUNT(*)
FROM table_name
GROUP BY partition_column;

--2查看表中不同列的数据分布情况：
SELECT column_name, COUNT(*)
FROM table_name
GROUP BY column_name;

--3查看执行计划：
EXPLAIN SELECT * FROM table_name WHERE partition_column = 'value';

--4查看表的索引情况：
SHOW INDEX FROM table_name;

--5查看表的统计信息：
ANALYZE TABLE table_name;



