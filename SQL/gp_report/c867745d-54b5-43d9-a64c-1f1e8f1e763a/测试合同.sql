select 日期                                                          event_date,
           员工工号                                                      usercode,
           产品线名称                                                    cpxname,
           产品名称                                                      cpname,
           COUNT(distinct id)                                         AS total_num,--总数量,
           count(distinct (CASE WHEN xqzt = 0 THEN id ELSE null END)) AS weichuli_num--未处理需求数量


select tjsj                                                          event_date,
       '员工工号'                                                    usercode,
       产品线名称                                                    cpxname,
       产品名称                                                      cpname,
       COUNT(distinct id)                                         AS total_num,--总数量,
       count(distinct (CASE WHEN xqzt = 0 THEN id ELSE null END)) AS weichuli_num--未处理需求数量
from (select regexp_split_to_table(concat_ws(',', t1.tjr, t1.cpjl, t1.reviewer), ',') as usercode,
             tjsj, --日期
             t1.id,
             t1.xqzt,
             t1.cpx                                                                      需求表产品线ID,
             t1.cpxcpbd                                                                  需求表产品ID,
             t2.cpxmc                                                                 as 产品线名称,
             t3.name                                                                  as 产品名称
      from ds_ecology_uf_IPDxqgl as t1
               left join ds_ecology_uf_productline as t2 on t1.cpx = t2.cpxid
               left join ds_ecology_uf_product as t3 on t1.cpxcpbd = t3.cpid
      where date(t1.tjsj) >= date('2022-01-01')
        and date(t1.tjsj) < date(current_date)
      group by regexp_split_to_table(concat_ws(',', t1.tjr, t1.cpjl, t1.reviewer), ','),
               t1.tjsj, t1.id, t1.xqzt, t1.cpx, t1.cpxcpbd, t2.cpxmc, t3.name) as tt
group by tjsj, 产品线名称, 产品名称
