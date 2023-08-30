--
WITH RECURSIVE
temp_t() AS (select t1.date_y,--年份
                       t1.date_q,--季度
                       t1.date_m,--月份
                       t1.pro_line_id,--产品线ID
                       t1.pro_id,--产品ID
                       t1.pro_linename,--产品线名称
                       t1.pro_name,--产品名称
                       case_num,-- 用例总数
                       new_case_num,--新增用例总数
                       auto_num,-- 自动化用例数
                       new_auto_num --自动化用例新增数
                from (select year    as date_y,
                             quarter as date_q,
                             month      date_m,
                             t1.cpxid   pro_line_id,--产品线ID
                             t1.cpid    pro_id,--产品ID
                             t1.cpxmc   pro_linename,--产品线名称
                             t1.cpname  pro_name --产品名称
                      from (select cpxid,
                                   cpxmc,
                                   cpid,
                                   name cpname
                            from ex_ods_pass_ecology_uf_productline as t1
                                     left join ex_ods_pass_ecology_uf_product as t2 on t1.cpxid = t2.linaname
                            where cpxid in ('03', '02', '04', '05', '10')) as t1
                               cross join (SELECT substr(to_char(dates, 'YYYY-MM-DD'), 1, 4)  AS year,
                                                  EXTRACT('QUARTER' from cast(dates as date)) AS quarter,
                                                  substr(to_char(dates, 'YYYY-MM-DD'), 6, 2)  AS month
                                           FROM (SELECT GENERATE_SERIES('2021-01-01'::DATE, current_date::DATE,
                                                                        '1 month'::INTERVAL) AS dates) subquery) as t2) as t1
                         left join (with temp_t as (select substr(to_char(t1.event_date, 'YYYY-MM-DD'), 1, 4)    date_y,
                                                           EXTRACT('QUARTER' from cast(t1.event_date as date))   date_q,
                                                           substr(to_char(t1.event_date, 'YYYY-MM-DD'), 6, 2) as date_m,
                                                           t1.cpxid                                              pro_line_id,
                                                           t1.cpid                                               pro_id,
                                                           t1.cpxmc                                           as pro_linename,
                                                           t1.cpname                                          as pro_name,
                                                           count(distinct t1.id)                                 case_num-- 用例总数
                                                    from (select cp.*, t3.*, date(t3.openeddate) as event_date
                                                          from (select cpxid,
                                                                       cpxmc,
                                                                       cpid,
                                                                       name cpname
                                                                from ex_ods_pass_ecology_uf_productline as t1
                                                                         left join ex_ods_pass_ecology_uf_product as t2 on t1.cpxid = t2.linaname
                                                                where cpxid in ('03', '02', '04', '05', '10')) cp
                                                                   left join ods_zentao_zt_module as t1 on t1.produtctid = cp.cpid
                                                                   left join ods_zentao_zt_product as t2 on t2.line = t1.id
                                                                   left join ods_zentao_zt_case as t3 on t3.product = t2.id
                                                          where t2.deleted = '0'
                                                            and t3.deleted = '0') as t1
                                                    group by substr(to_char(t1.event_date, 'YYYY-MM-DD'), 1, 4),
                                                             EXTRACT('QUARTER' from cast(t1.event_date as date)),
                                                             substr(to_char(t1.event_date, 'YYYY-MM-DD'), 6, 2),
                                                             t1.cpxid,
                                                             t1.cpid,
                                                             t1.cpxmc,
                                                             t1.cpname)
                                    SELECT distinct date_y,
                                                    date_q,
                                                    date_m,
                                                    pro_line_id,
                                                    pro_id,
                                                    pro_linename,
                                                    pro_name,
                                                    case_num                                                                                      new_case_num,
                                                    sum(case_num)
                                                    OVER (partition by pro_line_id,pro_id,pro_linename,pro_name ORDER BY date_y,date_q,date_m) AS case_num
                                    FROM temp_t) as t2
                                   on t1.date_y = t2.date_y and t1.date_q = t2.date_q and
                                      cast(t1.date_m as text) = t2.date_m and
                                      t1.pro_line_id = t2.pro_line_id and t1.pro_id = t2.pro_id
                         left join (with temp_t as (select substr(to_char(t1.event_date, 'YYYY-MM-DD'), 1, 4)    date_y,
                                                           EXTRACT('QUARTER' from cast(t1.event_date as date))   date_q,
                                                           substr(to_char(t1.event_date, 'YYYY-MM-DD'), 6, 2) as date_m,
                                                           t1.cpxid                                              pro_line_id,
                                                           t1.cpid                                               pro_id,
                                                           t1.cpxmc                                           as pro_linename,
                                                           t1.cpname                                          as pro_name,
                                                           count(distinct t1.id)                                 auto_num-- 自动化用例数
                                                    from (select cpxid,
                                                                 cpxmc,
                                                                 cpid,
                                                                 cpname,
                                                                 id,
                                                                 min(event_date) as event_date
                                                          from (select cp.cpxid,
                                                                       cp.cpxmc,
                                                                       cp.cpid,
                                                                       cp.cpname,
                                                                       t3.id,
                                                                       t3.SFZDH,
                                                                       date(t4.date) as           action_date,
                                                                       date(t3.openeddate),
                                                                       CASE
                                                                           WHEN date(t4.date) is null
                                                                               then date(t3.openeddate)
                                                                           else date(t4.date) end event_date
                                                                from (select cpxid,
                                                                             cpxmc,
                                                                             cpid,
                                                                             name cpname
                                                                      from ex_ods_pass_ecology_uf_productline as t1
                                                                               left join ex_ods_pass_ecology_uf_product as t2 on t1.cpxid = t2.linaname
                                                                      where cpxid in ('03', '02', '04', '05', '10')) cp
                                                                         left join ods_zentao_zt_module as t1 on t1.produtctid = cp.cpid
                                                                         left join ods_zentao_zt_product as t2 on t2.line = t1.id
                                                                         left join ods_zentao_zt_case as t3 on t3.product = t2.id
                                                                         left join
                                                                     (select t1.objectid, t1.date, t1.id
                                                                      from ods_zentao_zt_action as t1
                                                                               join ods_zentao_zt_history as t2 on t1.id = t2.action
                                                                      where t1.objectType = 'case'
                                                                        AND t1.action in ('edited')
                                                                        AND t2.new = 'a1'
                                                                        AND t2.field = 'SFZDH') as t4
                                                                     on t3.id = t4.objectID
                                                                where t2.deleted = '0'
                                                                  and t3.deleted = '0'
                                                                  and t3.SFZDH = 'a1') as t
                                                          group by cpxid,
                                                                   cpxmc,
                                                                   cpid,
                                                                   cpname,
                                                                   id) as t1
                                                    group by substr(to_char(t1.event_date, 'YYYY-MM-DD'), 1, 4),
                                                             EXTRACT('QUARTER' from cast(t1.event_date as date)),
                                                             substr(to_char(t1.event_date, 'YYYY-MM-DD'), 6, 2),
                                                             t1.cpxid,
                                                             t1.cpid,
                                                             t1.cpxmc,
                                                             t1.cpname)
                                    SELECT distinct date_y,
                                                    date_q,
                                                    date_m,
                                                    pro_line_id,
                                                    pro_id,
                                                    pro_linename,
                                                    pro_name,
                                                    auto_num                                                                                      new_auto_num,
                                                    sum(auto_num)
                                                    OVER (partition by pro_line_id,pro_id,pro_linename,pro_name ORDER BY date_y,date_q,date_m) AS auto_num
                                    FROM temp_t) as t3
                                   on t1.date_y = t3.date_y and t1.date_q = t3.date_q and
                                      cast(t1.date_m as text) = t3.date_m and
                                      t1.pro_line_id = t3.pro_line_id and t1.pro_id = t3.pro_id),
 filled_temp_t AS (
    SELECT
        date_y,
        date_q,
        date_m,
        pro_line_id,
        pro_id,
        pro_linename,
        pro_name,
        case_num,
        new_case_num,
        auto_num,
        new_auto_num,
        CASE
            WHEN case_num IS NULL THEN 1
            ELSE 0
        END AS is_null,
        ROW_NUMBER() OVER (PARTITION BY pro_line_id, pro_id, pro_linename, pro_name ORDER BY date_y, date_q, date_m) AS rn
    FROM temp_t
),
recursive_cte AS (
    SELECT
        date_y,
        date_q,
        date_m,
        pro_line_id,
        pro_id,
        pro_linename,
        pro_name,
        case_num,
        new_case_num,
        auto_num,
        new_auto_num,
        is_null,
        rn
    FROM filled_temp_t
    WHERE rn = 1
    UNION ALL
    SELECT
        t1.date_y,
        t1.date_q,
        t1.date_m,
        t1.pro_line_id,
        t1.pro_id,
        t1.pro_linename,
        t1.pro_name,
        COALESCE(t1.case_num, t2.case_num),
        COALESCE(t1.new_case_num, t2.new_case_num),
        COALESCE(t1.auto_num, t2.auto_num),
        COALESCE(t1.new_auto_num, t2.new_auto_num),
        CASE
            WHEN t1.case_num IS NULL AND t2.is_null = 1 THEN 1
            ELSE 0
        END,
        t1.rn
    FROM filled_temp_t t1
    JOIN recursive_cte t2 ON t1.rn = t2.rn + 1 AND t1.pro_line_id = t2.pro_line_id AND t1.pro_id = t2.pro_id AND t1.pro_linename = t2.pro_linename AND t1.pro_name = t2.pro_name
)
SELECT distinct
    date_y,
    date_q,
    date_m,
    pro_line_id,
    pro_id,
    pro_linename,
    pro_name,
    case_num,
    new_case_num,
    auto_num,
    new_auto_num
FROM recursive_cte;




