--发布版本数-月份 上线
select substr(t1.fbtgsj, 1, 4)           date_y,
       substr(t1.fbtgsj, 6, 2) as        date_m,
       t1.cpx                            需求表产品线ID,
       t1.xzcp                           需求表产品ID,
       t2.cpxmc                as        产品线名称,
       t3.name                 as        产品名称,
       case
           when bblx = 0 then '大版本'
           when bblx = 1 then '小版本'
           when bblx = 2 then '定制'
           when bblx = 3 then '补丁' end 版本类型,
       count(distinct t1.id)   as        num
from ex_ods_pass_ecology_uf_Release as t1
         left join ex_ods_pass_ecology_uf_productline as t2 on t1.cpx = t2.cpxid
         left join ex_ods_pass_ecology_uf_product as t3 on t1.xzcp = t3.cpid
where fbtgsj is not null
group by substr(t1.fbtgsj, 1, 4), substr(t1.fbtgsj, 6, 2), t1.cpx,
         case
             when bblx = 0 then '大版本'
             when bblx = 1 then '小版本'
             when bblx = 2 then '定制'
             when bblx = 3 then '补丁' end,
         t1.xzcp,
         t2.cpxmc,
         t3.name

--版本类型： 0 大 版本  1 小版本   2 定制   3 补丁
--产品线	  cpx
--产品     xzcp
--sqrq 申请日期,
--fbtgsj 发布通过时间,

--总需求数
select substr(t1.tjsj, 1, 4) || substr(t1.tjsj, 5, 3) || '-' || extract(week from date(t1.tjsj)) tjsj,       --日期
       t1.cpx                                                                                    需求表产品线ID,
       t1.cpxcpbd                                                                                需求表产品ID,
       t2.cpxmc                                                      as                          产品线名称,
       t3.name                                                       as                          产品名称,
       COUNT(distinct t1.id)                                         AS                          total_num,--总数量,
       count(distinct case
                          when xqzt in (0, 1) and xqlb = 0 and sfdz = 1 and date(csrq) > date(current_date) then t1.id
                          when xqzt in (0, 1) and xqlb = 1 and date(csrq) > date(current_date) then t1.id
                          else null end),-- 逾期未处理市场需求数
       count(distinct case when pqjf = 1 and date(yjjfrq) > date(current_date) then t1.id else null end),--逾期未交付市场需求数
       count(distinct (CASE WHEN xqzt = 0 THEN t1.id ELSE null END)) AS                          weichuli_num--未处理需求数量
from ex_ods_pass_ecology_uf_IPDxqgl as t1
         left join ex_ods_pass_ecology_uf_productline as t2 on t1.cpx = t2.cpxid
         left join ex_ods_pass_ecology_uf_product as t3 on t1.cpxcpbd = t3.cpid
where date(t1.tjsj) >= date('2022-01-01')
  and date(t1.tjsj) <= date(current_date)
group by substr(t1.tjsj, 1, 4) || substr(t1.tjsj, 5, 3) || '-' || extract(week from date(t1.tjsj))
       , t1.cpx, t1.cpxcpbd, t2.cpxmc, t3.name


--已处理市场需求数
--还是通过提交时间来聚合
select substr(t4.tjsj, 1, 4) || substr(t4.tjsj, 5, 3) || '-' || extract(week from date(t4.tjsj))   date_w,
       t4.cpx                                                                                      需求表产品线ID,
       t4.cpxcpbd                                                                                  需求表产品ID,
       t2.cpxmc                                                                                 as 产品线名称,
       t3.name                                                                                  as 产品名称,
       count(distinct case when lx in (1, 5) and xqzt in (1, 2, 3, 4) then t4.id else null end) as 已处理市场需求数,
       count(distinct case when lx in (2, 3) and pqjf <> 0 then t4.id else null end)            as 已排期市场需求数
from ex_ods_pass_ecology_BusinessLog_view as t1
         left join ex_ods_pass_ecology_uf_IPDxqgl t4 on cast(t4.id as text) = t1.rrxq
         left join ex_ods_pass_ecology_uf_productline as t2 on t4.cpx = t2.cpxid
         left join ex_ods_pass_ecology_uf_product as t3 on t4.cpxcpbd = t3.cpid
where date(t4.tjsj) >= date('2022-01-01')
  and date(t4.tjsj) <= date(current_date)
  and t1.clr is not null
  and (t4.cpjl like concat('%', cast(t1.clr as text), '%') or t4.reviewer like concat('%', cast(t1.clr as text), '%'))
  and t4.cpx in ('03', '02', '04', '05')
group by substr(t4.tjsj, 1, 4) || substr(t4.tjsj, 6, 2) || '-' || extract(week from date(t4.tjsj)),
         t4.cpx,
         t4.cpxcpbd,
         t2.cpxmc,
         t3.name



