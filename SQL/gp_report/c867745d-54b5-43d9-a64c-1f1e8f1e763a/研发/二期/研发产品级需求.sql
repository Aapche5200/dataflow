---1、产品发布分析
--①发布类型统计
truncate table ads_release_version_qty;
insert into ads_release_version_qty
select substr(t1.fbtgsj, 1, 4)                         date_y,--年份
       EXTRACT('QUARTER' from cast(t1.fbtgsj as date)) date_q,--季度
       substr(t1.fbtgsj, 6, 2)  as                     date_m,--月份
       t1.cpx                                          pro_line_id,--产品线ID
       t1.xzcp                                         pro_id,--产品ID
       t2.cpxmc                   as                     pro_linename,--产品线名称
       t3.name                  as                     pro_name,--产品名称
       case
           when bblx = 0 then '大版本'
           when bblx = 1 then '小版本'
           when bblx = 2 then '定制'
           when bblx = 3 then '补丁' end               version_type,--版本类型
       count(distinct t1.bbhzw) as                     version_num --发布数量
from ex_ods_pass_ecology_uf_Release as t1
         left join ex_ods_pass_ecology_uf_productline  as t2 on t1.cpx = t2.cpxid
         left join ex_ods_pass_ecology_uf_product as t3 on t1.xzcp = t3.cpid
where fbtgsj is not null
  and t1.cpx in ('03', '02', '22','23','24', '05', '10') and t1.xzcp not in ('022','030','082','029','028','123','027')
group by substr(t1.fbtgsj, 1, 4),
         EXTRACT('QUARTER' from cast(t1.fbtgsj as date)),
         substr(t1.fbtgsj, 6, 2), t1.cpx, t1.xzcp,
         t2.cpxmc,
         t3.name ,
         case
             when bblx = 0 then '大版本'
             when bblx = 1 then '小版本'
             when bblx = 2 then '定制'
             when bblx = 3 then '补丁' end
;

--②发布版本上车禅道趋势
-- 1、根据：在统计周期内从版本发布平台获取已发布版本 AND 类型为="大版本、小版本、定制版本“的版本名称、发布版本数，因为版本发布平台是一个个的发布单，若出现多个版本重复发布，仅版本首次发布参与统计
-- 2、根据：产品基础信息表.产品编码= zt_module.productId 找到产品的zt_module.id
-- 3、根据：zt_module.id = zt.product.line AND zt.product.deleted=0 找到对应产品的需求库的id（可能会有多个）
-- 4、根据： zt_productplan.product= zt.product ，找到 zt_productplan.title，即找到产品对应的产品版本名称
-- 5、根据： zt_productplan.title  判断与“版本平台中发布版本的名称”是否存在，在存在的情况下取产品版本的编码 zt_productplan.id
-- 6、根据：zt_productplan.id=zt_projectproduct.plan 找到zt_projectproduct.project,获取产品版本关联的项目id
-- 7、根据：zt_project.id=zt_projectproduct.project  AND zt_project.deleted = 0 找到zt_project.type,获取项目关联的项目类型
-- 8、根据：zt_project.model= "scrum" 或"waterfall" ，判断出当前发布的版本使用了禅道开发
-- 9、根据：使用禅道开发的版本数/发布周期内版本总数，得出上线禅道的比例
truncate table ads_zentao_version_qty;
insert into ads_zentao_version_qty
select substr(t1.fbtgsj, 1, 4)                         date_y,--年份
       EXTRACT('QUARTER' from cast(t1.fbtgsj as date)) date_q,--季度
       substr(t1.fbtgsj, 6, 2) as                      date_m,--月份
       t1.cpx                                        pro_line_id,--产品线ID
       t1.xzcp                                         pro_id,--产品ID
       t3.cpxmc                as                      pro_linename,--产品线名称
       t4.name                as                      pro_name,--产品名称
       null                                            version_type,--版本类型
       round(cast(count(distinct t2.spc_name ) as numeric) / CAST(count(distinct t1.bbhzw) AS numeric),4)  as      version_num --发布数量
from ex_ods_pass_ecology_uf_Release as t1
         left join ex_ods_pass_ecology_uf_productline as t3 on t1.cpx = t3.cpxid
         left join ex_ods_pass_ecology_uf_product as t4 on t1.xzcp = t4.cpid
         left join (select cp.*, t3.title as spc_name, t5.*
                    from (select t1.cpxid as cpxid,
                                 cpxmc      as cpxmc,
                                 cpid        cpid,
                                 name         cpname
                          from ex_ods_pass_ecology_uf_productline as t1
                                   left join ex_ods_pass_ecology_uf_product as t2 on t1.cpxid = t2.linaname
                          where t1.cpxid  in ('03', '02', '22','23','24', '05','10') and cpid  not in ('022','030','082','029','028','123','027')
                          ) cp
                             left join ods_zentao_zt_module as t1 on t1.produtctid = cp.cpid
                             left join ods_zentao_zt_product as t2 on t2.line = t1.id
                             left join ods_zentao_zt_productplan as t3 on t3.product = t2.id
                             left join (select case
                                                   when length(plan) - length(replace(plan, ',', '')) = 2
                                                       then split_part(plan, ',', 2)
                                                   when length(plan) - length(replace(plan, ',', '')) = 3
                                                       then split_part(plan, ',', 3)
                                                   else plan end plan,
                                               project
                                        from ods_zentao_zt_projectproduct) as t4 on t4.plan = cast(t3.id as text)
                             left join ods_zentao_zt_project as t5 on t5.id = t4.project
                    where t2.deleted = '0'
                      and (t5.model = 'waterfall' or t5.model = 'scrum')
                      and t5.deleted = '0') as t2 on t1.bbhzw = t2.spc_name and t1.cpx = t2.cpxid and t1.xzcp = t2.cpid
where t1.cpx in ('03', '02', '22','23','24', '05', '10')
group by substr(t1.fbtgsj, 1, 4),
         EXTRACT('QUARTER' from cast(t1.fbtgsj as date)),
         substr(t1.fbtgsj, 6, 2),
         t1.cpx,
         t1.xzcp,
         t3.cpxmc,
         t4.name;



--③版本逾期发布统计--无数据--开始
truncate table ads_overdue_version_qty;
insert into ads_overdue_version_qty
select substr(t1.fbtgsj, 1, 4)                                                           date_y,--年份
       EXTRACT('QUARTER' from cast(t1.fbtgsj as date))                                   date_q,--季度
       substr(t1.fbtgsj, 6, 2)                                                        as date_m,--月份
       t2.cpxid                                                                          pro_line_id,--产品线ID
       t2.cpid                                                                           pro_id,--产品ID
       t2.cpxmc                                                                       as pro_linename,--产品线名称
       t2.cpname                                                                      as pro_name,--产品名称
       count(distinct case when date_plan < date(t1.fbtgsj) then t1.bbhzw else null end) as version_num --发布数量
from ex_ods_pass_ecology_uf_Release as t1
         left join (select cp.*, t3.title as spc_name, t3.id, date(t3.end) as date_plan
                    from (select t1.cpxid as cpxid,
                                 cpxmc      as cpxmc,
                                 cpid        cpid,
                                 name         cpname
                          from ex_ods_pass_ecology_uf_productline as t1
                                   left join ex_ods_pass_ecology_uf_product as t2 on t1.cpxid = t2.linaname
                          where t1.cpxid  in ('03', '02', '22','23','24', '05','10') and cpid  not in ('022','030','082','029','028','123','027')
                          ) cp
                             left join ods_zentao_zt_module as t1 on t1.produtctid = cp.cpid
                             left join ods_zentao_zt_product as t2 on t2.line = t1.id
                             left join ods_zentao_zt_productplan as t3 on t3.product = t2.id
                    where t2.deleted = '0'
) as t2 on t1.bbhzw = t2.spc_name and t1.cpx = t2.cpxid and t1.xzcp = t2.cpid
where cpxid is not null and t1.cpx in ('03', '02', '22','23','24', '05','10')
group by substr(t1.fbtgsj, 1, 4),
         EXTRACT('QUARTER' from cast(t1.fbtgsj as date)),
         substr(t1.fbtgsj, 6, 2),
         t2.cpxid,
         t2.cpid,
         t2.cpxmc,
         t2.cpname;

--④发布类型统计表格--计划发布日期是空的xx
truncate table ads_release_table_qty;
insert into ads_release_table_qty
select substr(t1.fbtgsj, 1, 4)                         date_y,--年份
       EXTRACT('QUARTER' from cast(t1.fbtgsj as date)) date_q,--季度
       substr(t1.fbtgsj, 6, 2) as                      date_m,--月份
       t1.cpx                                          pro_line_id,--产品线ID
       t1.xzcp                                         pro_id,--产品ID
       t2.cpxmc                as                      pro_linename,--产品线名称
       t3.name                 as                      pro_name,--产品名称
       bbhzw                                           release_version,--发布版本
       case
           when bblx = 0 then '大版本'
           when bblx = 1 then '小版本'
           when bblx = 2 then '定制'
           when bblx = 3 then '补丁' end               version_type,--版本类型
       date(fbtgsj)            as                      act_date,--实际发布日期
       date_plan                                       plan_date,--计划发布日期
       t6.id                                           zentao_id,--关联禅道项目ID
       t5.lastname                                     pro_manager,--产品经理
       t5.workcode                                     pro_manager_code,--产品经理工号
       t5.lastname || '(' || t5.workcode || ')'        manager_namecode,--经理姓名工号
       t4.lastname                                     release_owner,--发布人
       t4.workcode                                     release_code,--发布人工号
       t4.lastname || '(' || t4.workcode || ')'        release_namecode, --发布人姓名工号
       date(t1.sqrq)           as                      create_date --发布创建日期
from ex_ods_pass_ecology_uf_Release as t1
         left join ex_ods_pass_ecology_uf_productline as t2 on t1.cpx = cast(t2.cpxid as text)
         left join ex_ods_pass_ecology_uf_product as t3 on t1.xzcp = cast(t3.cpid as text)
         left join ex_ods_pass_ecology_hrmresource as t4 on t1.sqr = t4.id
         left join ex_ods_pass_ecology_hrmresource as t5 on t3.cpjlx = t5.id
         left join (select cp.*, t3.title as spc_name, t5.id, date(t3.end) as date_plan
                    from (select t1.cpxid as cpxid,
                                 cpxmc    as cpxmc,
                                 cpid        cpid,
                                 name        cpname
                          from ex_ods_pass_ecology_uf_productline as t1
                                   left join ex_ods_pass_ecology_uf_product as t2
                                             on t1.cpxid = t2.linaname
                          where t1.cpxid in ('03', '02', '22','23','24', '05', '10')
                            and cpid not in
                                ('022', '030', '082', '029', '028', '123', '027')) cp
                             left join ods_zentao_zt_module as t1 on t1.produtctid = cp.cpid
                             left join ods_zentao_zt_product as t2 on t2.line = t1.id
                             left join ods_zentao_zt_productplan as t3 on t3.product = t2.id
                             left join (select case
                                                   when length(plan) - length(replace(plan, ',', '')) = 2
                                                       then split_part(plan, ',', 2)
                                                   when length(plan) - length(replace(plan, ',', '')) = 3
                                                       then split_part(plan, ',', 3)
                                                   else plan end plan,
                                               project
                                        from ods_zentao_zt_projectproduct) as t4
                                       on t4.plan = cast(t3.id as text)
                             left join ods_zentao_zt_project as t5 on t5.id = t4.project
                    where t2.deleted = '0'
                      and (t5.model = 'waterfall' or t5.model = 'scrum')
                      and t5.deleted = '0') as t6
                   on t1.bbhzw = t6.spc_name and t1.cpx = t6.cpxid and t1.xzcp = t6.cpid
where fbtgsj is not null
  and t1.cpx in ('03', '02', '22','23','24', '05', '10')
  and t1.xzcp not in ('022', '030', '082', '029', '028', '123', '027')
group by substr(t1.fbtgsj, 1, 4),
         EXTRACT('QUARTER' from cast(t1.fbtgsj as date)),
         substr(t1.fbtgsj, 6, 2),
         t1.cpx,
         t1.xzcp,
         t2.cpxmc,
         t3.name,
         bbhzw,
         case
             when bblx = 0 then '大版本'
             when bblx = 1 then '小版本'
             when bblx = 2 then '定制'
             when bblx = 3 then '补丁' end,
         date(fbtgsj),
         date_plan,
         t6.id,
         t5.lastname,
         t5.workcode,
         t5.lastname || '(' || t5.workcode || ')',
         t4.lastname,
         t4.workcode,
         t4.lastname || '(' || t4.workcode || ')',
         date(t1.sqrq);

---2、产品研发bug分析
--①新增Bug严重程度统计
truncate table ads_bug_level_qty;
insert into ads_bug_level_qty
select substr(to_char(event_date, 'YYYY-MM-DD'), 1, 4)  date_y,--年份
       EXTRACT('QUARTER' from cast(event_date as date)) date_q, --季度
       substr(to_char(event_date, 'YYYY-MM-DD'), 6, 2)  date_m, --月份
       cpxid                                            pro_line_id, --产品线ID
       cpxmc                                            pro_linename,--产品线名称
       cpid                                             pro_id,--产品ID
       cpname                                           pro_name,--产品名称
       case
           when severity = 1 then '致命'
           when severity = 2 then '严重'
           when severity = 3 then '一般'
           when severity = 4 then '建议'
           end                                          bug_level,--bug严重程度
       count(distinct id) as                            cp_bug_num--bug数量
from (select cpxid,
             cpxmc,
             cpid,
             cpname,
             module_id,
             pro_id,
             b.id,
             b.severity,
             b.product,
             b.status,
             b."activatedCount",
             date("openedDate") as event_date
      from (select cpxid,
                   cpxmc,
                   cpid,
                   cpname,
                   module_id,
                   p.id as pro_id
            from (select cpxid, cpxmc, cpid, cpname, m.id as module_id
                  from (select cpxid,
                               cpxmc,
                               cpid,
                               name cpname
                        from ex_ods_pass_ecology_uf_productline as t1
                                 left join ex_ods_pass_ecology_uf_product as t2 on t1.cpxid = t2.linaname
                        where cpxid in ('03', '02', '22','23','24', '05','10')
                          and cpid not in ('022','030','082','029','028','123','027')
                        ) oa_cp
                           join ods_zentao_zt_module as m on oa_cp.cpid = cast(m.produtctid as text)) as cpm
                     join ods_zentao_zt_product as p on cast(cpm.module_id as text) = cast(p.line as text)
            where p.deleted = '0') as cpmp
               join ex_ods_cd_zentao_zt_bug as b on cpmp.pro_id = b.product
      where b.deleted = '0'
        and b.origin != 'ITR'
        and date("openedDate") >= date('2022-01-01'))  as t
group by substr(to_char(event_date, 'YYYY-MM-DD'), 1, 4),
         EXTRACT('QUARTER' from cast(event_date as date)),
         substr(to_char(event_date, 'YYYY-MM-DD'), 6, 2),
         cpxid,
         cpxmc,
         cpid,
         cpname,
         case
             when severity = 1 then '致命'
             when severity = 2 then '严重'
             when severity = 3 then '一般'
             when severity = 4 then '建议'
             end;

--②新增二次激活Bug趋势
truncate table ads_bug_active_qty;
insert into ads_bug_active_qty
with temp_t as (select substr(to_char(event_date, 'YYYY-MM-DD'), 1, 4)              date_y,
                       EXTRACT('QUARTER' from cast(event_date as date))             date_q,
                       substr(to_char(event_date, 'YYYY-MM-DD'), 6, 2)              date_m,
                       cpxid,
                       cpxmc,
                       cpid,
                       cpname,
                       count(case when activatedcount > 0 then id else null end) as second_active_bug_nuim --产品二次激活bug数
                from (select cpxid,
                             cpxmc,
                             cpid,
                             cpname,
                             module_id,
                             pro_id,
                             b.id,
                             b.product,
                             b.status,
                             b."activatedCount" activatedcount,
                             date(case when substr(COALESCE(("activatedDate"::text),'0000-00-00'),1,10)='0000-00-00' then null
                                 else substr(COALESCE(("activatedDate"::text),'0000-00-00'),1,10) end ) as event_date
                      from (select cpxid,
                                   cpxmc,
                                   cpid,
                                   cpname,
                                   module_id,
                                   p.id as pro_id
                            from (select cpxid, cpxmc, cpid, cpname, m.id as module_id
                                  from (select cpxid,
                                               cpxmc,
                                               cpid,
                                               name cpname
                                        from ex_ods_pass_ecology_uf_productline as t1
                                                 left join ex_ods_pass_ecology_uf_product as t2 on t1.cpxid = t2.linaname
                                        where cpxid in ('03', '02', '22','23','24', '05','10') and cpid not in ('022','030','082','029','028','123','027')
                                        ) oa_cp
                                           join ods_zentao_zt_module as m on oa_cp.cpid = cast(m.produtctid as text)) as cpm
                                     join ods_zentao_zt_product as p
                                          on cast(cpm.module_id as text) = cast(p.line as text)
                            where p.deleted = '0') as cpmp
                               join ex_ods_cd_zentao_zt_bug as b on cpmp.pro_id = b.product
                      where b.deleted = '0'
                        and b.origin != 'ITR'
                        and date(case when substr(COALESCE(("activatedDate"::text),'0000-00-00'),1,10)='0000-00-00' then null
                            else substr(COALESCE(("activatedDate"::text),'0000-00-00'),1,10) end )  >= date('2022-01-01')) as t
                group by substr(to_char(event_date, 'YYYY-MM-DD'), 1, 4),
                         EXTRACT('QUARTER' from cast(event_date as date)),
                         substr(to_char(event_date, 'YYYY-MM-DD'), 6, 2),
                         cpxid,
                         cpxmc,
                         cpid,
                         cpname)
select t1.date_y,--年份
       t1.date_q,--季度
       t1.date_m,--月份
       t1.cpxid                                              pro_line_id,--产品线ID
       t1.cpxmc                                              pro_linename,--产品线名称
       t1.cpid                                               pro_id,--产品ID
       t1.cpname                                             pro_name,--产品名称
       t1.second_active_bug_nuim,--二次激活bug数量
       coalesce(t1.second_active_bug_nuim,0) - coalesce(t2.second_active_bug_nuim,0) new_bug_num--新增二次激活bug数量
from temp_t as t1
         left join temp_t as t2 on t1.date_y = t2.date_y
    and t1.cpxid = t2.cpxid
    and t1.cpid = t2.cpid
    and cast(t1.date_m as int) = cast(t2.date_m as int) + 1;




--季度：新增二次激活Bug趋势
truncate table ads_bug_active_quarterqty;
insert into ads_bug_active_quarterqty
with temp_t as (select substr(to_char(event_date, 'YYYY-MM-DD'), 1, 4)              date_y,
                       EXTRACT('QUARTER' from cast(event_date as date))             date_q,
                       cpxid,
                       cpxmc,
                       cpid,
                       cpname,
                       count(case when activatedcount > 0 then id else null end) as second_active_bug_nuim --产品二次激活bug数
                from (select cpxid,
                             cpxmc,
                             cpid,
                             cpname,
                             module_id,
                             pro_id,
                             b.id,
                             b.product,
                             b.status,
                             b."activatedCount" activatedcount,
                             date(case when substr(COALESCE(("activatedDate"::text),'0000-00-00'),1,10)='0000-00-00' then null
                                 else substr(COALESCE(("activatedDate"::text),'0000-00-00'),1,10) end )  as event_date
                      from (select cpxid,
                                   cpxmc,
                                   cpid,
                                   cpname,
                                   module_id,
                                   p.id as pro_id
                            from (select cpxid, cpxmc, cpid, cpname, m.id as module_id
                                  from (select cpxid,
                                               cpxmc,
                                               cpid,
                                               name cpname
                                        from ex_ods_pass_ecology_uf_productline as t1
                                                 left join ex_ods_pass_ecology_uf_product as t2 on t1.cpxid = t2.linaname
                                        where cpxid in ('03', '02', '22','23','24', '05','10') and cpid not in ('022','030','082','029','028','123','027')
                                        ) oa_cp
                                           join ods_zentao_zt_module as m on oa_cp.cpid = cast(m.produtctid as text)) as cpm
                                     join ods_zentao_zt_product as p
                                          on cast(cpm.module_id as text) = cast(p.line as text)
                            where p.deleted = '0') as cpmp
                               join ex_ods_cd_zentao_zt_bug as b on cpmp.pro_id = b.product
                      where b.deleted = '0'
                        and b.origin != 'ITR'
                        and date(case when substr(COALESCE(("activatedDate"::text),'0000-00-00'),1,10)='0000-00-00' then null
                            else substr(COALESCE(("activatedDate"::text),'0000-00-00'),1,10) end ) >= date('2022-01-01')) as t
                group by substr(to_char(event_date, 'YYYY-MM-DD'), 1, 4),
                         EXTRACT('QUARTER' from cast(event_date as date)),
                         cpxid,
                         cpxmc,
                         cpid,
                         cpname)
select t1.date_y,--年份
       t1.date_q,--季度
       t1.cpxid                                              pro_line_id,--产品线ID
       t1.cpxmc                                              pro_linename,--产品线名称
       t1.cpid                                               pro_id,--产品ID
       t1.cpname                                             pro_name,--产品名称
       t1.second_active_bug_nuim,--二次激活bug数量
       coalesce(t1.second_active_bug_nuim,0) - coalesce(t2.second_active_bug_nuim,0) new_bug_num--新增二次激活bug数量
from temp_t as t1
         left join temp_t as t2 on t1.date_y = t2.date_y
    and t1.cpxid = t2.cpxid
    and t1.cpid = t2.cpid
    and cast(t1.date_q as int) = cast(t2.date_q as int) + 1;

--③产品研发bug分析报表数据
truncate table ads_bug_table_qty;
insert into ads_bug_table_qty
with temp_t
         as (select substr(to_char(event_date, 'YYYY-MM-DD'), 1, 4)      date_y,--年份
                    EXTRACT('QUARTER' from cast(event_date as date))     date_q,--季度
                    substr(to_char(event_date, 'YYYY-MM-DD'), 6, 2)      date_m,--月份
                    cpxid                                                pro_line_id,--产品线ID
                    cpxmc                                                pro_linename,--产品线名称
                    cpid                                                 pro_id,--产品ID
                    cpname                                               pro_name,--产品名称
                    id,
                    title                                                bug_title,--   Bug标题,
                    case
                        when status = 'active' then '激活'
                        when status = 'resolved' then '已解决'
                        when status = 'closed' then '已关闭'
                        end                                              bug_status,--状态,
                    case
                        when "subStatus" = '1' then '激活'
                        when "subStatus" = '102' then '测试经理审核'
                        when "subStatus" = '103' then '研发经理审核'
                        when "subStatus" = '104' then '问题定位修改'
                        when "subStatus" = '105' then '修改实施审核'
                        when "subStatus" = '106' then '问题待决策'
                        when "subStatus" = '6' then '已解决待回归'
                        when "subStatus" = '601' then '回归测试'
                        when "subStatus" = '602' then '问题关闭确认'
                        when "subStatus" = '10' then '问题解决关闭'
                        when "subStatus" = '1002' then '非问题关闭'
                        when "subStatus" = '1003' then '重复问题关闭'
                        when "subStatus" = '1004' then '决策不解决关闭'
                        when "subStatus" = '1005' then '转需求关闭'
                        end                                              bug_substatus,--子状态,
                    effect_name                                          effect_version,-- 影响版本,
                    case
                        when severity = 1 then '致命'
                        when severity = 2 then '严重'
                        when severity = 3 then '一般'
                        when severity = 4 then '建议'
                        end                                              bug_level,--   bug严重等级,
                    event_date,--创建日期
                    realname                                             assignedto,--指派人
                    "activatedCount" as activatedcount,--激活次数
                    active_date                                          activateddate,--激活日期
                    case when "activatedCount" > 0 then '是' else '否' end activatebug, --是否二次激活bug
                    createowner                                          openedby,--创建人,
                    substr(COALESCE(("closedDate"::text),'0000-00-00'),1,10)     closeddate --bug的关闭时间取
             from (select cpxid,
                          cpxmc,
                          cpid,
                          cpname,
                          module_id,
                          pro_id,
                          b.*,
                          c.name              effect_name,
                          d.realname,
                          e.active_date,
                          coalesce(f.realname,"openedBy")          createowner,
                          date("openedDate") as event_date
                   from (select cpxid,
                                cpxmc,
                                cpid,
                                cpname,
                                module_id,
                                p.id as pro_id
                         from (select cpxid, cpxmc, cpid, cpname, m.id as module_id
                               from (select cpxid,
                                            cpxmc,
                                            cpid,
                                            name cpname
                                     from ex_ods_pass_ecology_uf_productline as t1
                                              left join ex_ods_pass_ecology_uf_product as t2 on t1.cpxid = t2.linaname
                                     where cpxid in ('03', '02', '22','23','24', '05', '10') and cpid not in ('022','030','082','029','028','123','027')
                                     ) oa_cp
                                        join ods_zentao_zt_module as m on oa_cp.cpid = cast(m.produtctid as text)) as cpm
                                  join ods_zentao_zt_product as p
                                       on cast(cpm.module_id as text) = cast(p.line as text)
                         where p.deleted = '0') as cpmp
                            join ex_ods_cd_zentao_zt_bug as b on cpmp.pro_id = b.product
                            left join ods_zentao_zt_build as c on b."openedBuild" = cast(c.id as text)
                            left join ods_zentao_zt_user as d on b."assignedTo" = d.account
                            left join ods_zentao_zt_user as f on b."openedBy" = f.account
                            left join (select objectID, date(date) active_date
                                       from ods_zentao_zt_action
                                       where objectType = 'bug'
                                         AND action = 'activated'
                                       group by objectID, date(date))
                       as e on b.id = e.objectID
                   where b.deleted = '0'
                     and b.origin != 'ITR'
                     and date("openedDate") >= date('2022-01-01')) as t
             group by substr(to_char(event_date, 'YYYY-MM-DD'), 1, 4),
                      EXTRACT('QUARTER' from cast(event_date as date)),
                      substr(to_char(event_date, 'YYYY-MM-DD'), 6, 2),
                      cpxid,
                      cpxmc,
                      cpid,
                      cpname,
                      id,
                      title,
                      case
                          when status = 'active' then '激活'
                          when status = 'resolved' then '已解决'
                          when status = 'closed' then '已关闭'
                          end,
                      case
                          when "subStatus" = '1' then '激活'
                          when "subStatus" = '102' then '测试经理审核'
                          when "subStatus" = '103' then '研发经理审核'
                          when "subStatus" = '104' then '问题定位修改'
                          when "subStatus" = '105' then '修改实施审核'
                          when "subStatus" = '106' then '问题待决策'
                          when "subStatus" = '6' then '已解决待回归'
                          when "subStatus" = '601' then '回归测试'
                          when "subStatus" = '602' then '问题关闭确认'
                          when "subStatus" = '10' then '问题解决关闭'
                          when "subStatus" = '1002' then '非问题关闭'
                          when "subStatus" = '1003' then '重复问题关闭'
                          when "subStatus" = '1004' then '决策不解决关闭'
                          when "subStatus" = '1005' then '转需求关闭'
                          end,
                      effect_name,
                      case
                          when severity = 1 then '致命'
                          when severity = 2 then '严重'
                          when severity = 3 then '一般'
                          when severity = 4 then '建议'
                          end,
                      event_date,
                      realname,
                      "activatedCount",
                      active_date,
                      case when "activatedCount" > 0 then '是' else '否' end, createowner,
                      substr(COALESCE(("closedDate"::text),'0000-00-00'),1,10)  )
select date_y,--年份
       date_q,--季度
       date_m,--月份
       pro_line_id,--产品线ID
       pro_linename,--产品线名称
       pro_id,--产品ID
       pro_name,--产品名称
       id,
       bug_title,--   Bug标题,
       bug_status,--状态,
       bug_substatus,--子状态,
       effect_version,-- 影响版本,
       bug_level,--   bug严重等级,
       event_date,--创建日期
       assignedto,--指派人
       activatedCount,--激活次数
       string_agg(substr(to_char(activatedDate, 'YYYY-MM-DD'), 1, 10), ',') activateddate,--激活日期
       activatebug,--是否二次激活bug
       openedby,--创建人
       closeddate --关闭日期
from temp_t as t
group by date_y,
         date_q,
         date_m,
         pro_line_id,
         pro_linename,
         pro_id,
         pro_name,
         id,
         bug_title,
         bug_status,
         bug_substatus,
         effect_version,
         bug_level,
         event_date,
         assignedto,
         activatedCount,
         activatebug,
         openedby,
         closeddate
;


--3、产品现网bug分析
--①现网bug新增数（不同严重程度）
truncate table ads_bug_itr_qty;
insert into ads_bug_itr_qty
select substr(to_char(event_date, 'YYYY-MM-DD'), 1, 4)  date_y,--年份
       EXTRACT('QUARTER' from cast(event_date as date)) date_q, --季度
       substr(to_char(event_date, 'YYYY-MM-DD'), 6, 2)  date_m, --月份
       cpxid                                            pro_line_id,--产品线ID
       cpxmc                                            pro_linename,--产品线名称
       cpid                                             pro_id,--产品ID
       cpname                                           pro_name,--产品名称
       case
           when severity = 1 then '致命'
           when severity = 2 then '严重'
           when severity = 3 then '一般'
           when severity = 4 then '建议'
           end                                          bug_level,--bug严重程度
       count(distinct id) as                            cp_bug_num--bug数量
from (select cpxid,
             cpxmc,
             cpid,
             cpname,
             module_id,
             pro_id,
             b.id,
             b.severity,
             b.product,
             b.status,
             b."activatedCount" activatedcount,
             date("openedDate") as event_date
      from (select cpxid,
                   cpxmc,
                   cpid,
                   cpname,
                   module_id,
                   p.id as pro_id
            from (select cpxid, cpxmc, cpid, cpname, m.id as module_id
                  from (select cpxid,
                               cpxmc,
                               cpid,
                               name cpname
                        from ex_ods_pass_ecology_uf_productline as t1
                                 left join ex_ods_pass_ecology_uf_product as t2 on t1.cpxid = t2.linaname
                        where cpxid in ('03', '02', '22','23','24', '05') and cpid not in ('022','030','082','029','028','123','027')
                        ) oa_cp
                           join ods_zentao_zt_module as m on oa_cp.cpid = cast(m.produtctid as text)) as cpm
                     join ods_zentao_zt_product as p on cast(cpm.module_id as text) = cast(p.line as text)
            where p.deleted = '0') as cpmp
               join ex_ods_cd_zentao_zt_bug as b on cpmp.pro_id = b.product
      where b.deleted = '0'
        and b.origin = 'ITR'
        and date("openedDate") >= date('2022-01-01')) as t
group by substr(to_char(event_date, 'YYYY-MM-DD'), 1, 4),
         EXTRACT('QUARTER' from cast(event_date as date)),
         substr(to_char(event_date, 'YYYY-MM-DD'), 6, 2),
         cpxid,
         cpxmc,
         cpid,
         cpname,
         case
             when severity = 1 then '致命'
             when severity = 2 then '严重'
             when severity = 3 then '一般'
             when severity = 4 then '建议'
             end;

--②新增Bug关联用例情况统计
truncate table ads_bug_related_qty;
insert into ads_bug_related_qty
select substr(to_char(event_date, 'YYYY-MM-DD'), 1, 4)  date_y,--年份
       EXTRACT('QUARTER' from cast(event_date as date)) date_q,--季度
       substr(to_char(event_date, 'YYYY-MM-DD'), 6, 2)  date_m,--月份
       cpxid                                            pro_line_id,--产品线ID
       cpxmc                                            pro_linename,--产品线名称
       cpid                                             pro_id,--产品ID
       cpname                                           pro_name,--产品名称
       case
           when "case" = 0 or "case" is null then '未关联'
           else '关联'
           end                                          related, --是否关联,
       count(distinct id) as                            cp_bug_num--bug数量
from (select cpxid,
             cpxmc,
             cpid,
             cpname,
             module_id,
             pro_id,
             b.id,
             b.case,
             b.product,
             b.status,
             b."activatedCount",
             date("openedDate") as event_date
      from (select cpxid,
                   cpxmc,
                   cpid,
                   cpname,
                   module_id,
                   p.id as pro_id
            from (select cpxid, cpxmc, cpid, cpname, m.id as module_id
                  from (select cpxid,
                               cpxmc,
                               cpid,
                               name cpname
                        from ex_ods_pass_ecology_uf_productline as t1
                                 left join ex_ods_pass_ecology_uf_product as t2 on t1.cpxid = t2.linaname
                        where cpxid in ('03', '02', '22','23','24', '05') and cpid not in ('022','030','082','029','028','123','027')
                        ) oa_cp
                           join ods_zentao_zt_module as m on oa_cp.cpid = cast(m.produtctid as text)) as cpm
                     join ods_zentao_zt_product as p on cast(cpm.module_id as text) = cast(p.line as text)
            where p.deleted = '0') as cpmp
               join ex_ods_cd_zentao_zt_bug as b on cpmp.pro_id = b.product
      where b.deleted = '0'
        and b.origin = 'ITR'
        and date("openedDate") >= date('2022-01-01')) as t
group by substr(to_char(event_date, 'YYYY-MM-DD'), 1, 4),
         EXTRACT('QUARTER' from cast(event_date as date)),
         substr(to_char(event_date, 'YYYY-MM-DD'), 6, 2),
         cpxid,
         cpxmc,
         cpid,
         cpname,
         case
             when "case" = 0 or "case" is null then '未关联'
             else '关联'
             end;

--③Bug解决时长趋势
truncate table ads_bug_itr_days;
insert into ads_bug_itr_days
select substr(to_char(event_date, 'YYYY-MM-DD'), 1, 4)  date_y,--年份
       EXTRACT('QUARTER' from cast(event_date as date)) date_q,--季度
       substr(to_char(event_date, 'YYYY-MM-DD'), 6, 2)  date_m,--月份
       cpxid                                            pro_line_id,--产品线ID
       cpxmc                                            pro_linename,--产品线名称
       cpid                                             pro_id,--产品ID
       cpname                                           pro_name,--产品名称
       avg(day_num) as                                  avg_day_num --平均bug解决时长
from (select cpxid, cpxmc, cpid, cpname, id, close_date as event_date, close_date - opend_date + 1 as day_num
      from (select cpxid,
                   cpxmc,
                   cpid,
                   cpname,
                   module_id,
                   pro_id,
                   b.id,
                   b.severity,
                   b.product,
                   b.status,
                   b."activatedCount",
                   date("openedDate") as opend_date,
                   date("closedDate") as close_date
            from (select cpxid,
                         cpxmc,
                         cpid,
                         cpname,
                         module_id,
                         p.id as pro_id
                  from (select cpxid, cpxmc, cpid, cpname, m.id as module_id
                        from (select cpxid,
                                     cpxmc,
                                     cpid,
                                     name cpname
                              from ex_ods_pass_ecology_uf_productline as t1
                                       left join ex_ods_pass_ecology_uf_product as t2 on t1.cpxid = t2.linaname
                              where cpxid in ('03', '02', '22','23','24', '05') and cpid not in ('022','030','082','029','028','123','027')
                              ) oa_cp
                                 join ods_zentao_zt_module as m on oa_cp.cpid = cast(m.produtctid as text)) as cpm
                           join ods_zentao_zt_product as p on cast(cpm.module_id as text) = cast(p.line as text)
                  where p.deleted = '0') as cpmp
                     join ex_ods_cd_zentao_zt_bug as b on cpmp.pro_id = b.product
            where b.deleted = '0'
              and b.origin = 'ITR'
              AND b.status = 'closed'
              and date("closedDate") >= date('2022-01-01')) as t
      group by cpxid, cpxmc, cpid, cpname, id, close_date, close_date - opend_date + 1) as tt
group by substr(to_char(event_date, 'YYYY-MM-DD'), 1, 4),
         EXTRACT('QUARTER' from cast(event_date as date)),
         substr(to_char(event_date, 'YYYY-MM-DD'), 6, 2),
         cpxid, cpxmc, cpid, cpname;

--④截至昨日未关闭Bug分布
-- 1.根据产品基础信息表.产品编码= zt_module.productId 找到产品的zt_module.id
-- 2.根据：zt_module.id = zt.product.line AND zt.product.deleted=0 找到对应产品的bug库的id（可能会有多个）
-- 3.根据：zt_bug.product=zt.product.id AND zt_bug.deleted=0  AND  zt_bug.origin=ITR AND  zt_bug.status ！="closed"找到所有ITR转bug且未关闭的bug
-- 4. 根据：今日-zt_bug.openedDate创建时间+1，算出bug的滞留时长
-- 5. 根据：根据bug的滞留时长分布“1~15天，15~30天，30~45天，>=45天”，来统计出不同滞留时长分布的bug数
--滞留时长分布
truncate table ads_bug_detain_days;
insert into ads_bug_detain_days
select substr(to_char(event_date, 'YYYY-MM-DD'), 1, 4)  date_y,--年份
       EXTRACT('QUARTER' from cast(event_date as date)) date_q, --季度
       substr(to_char(event_date, 'YYYY-MM-DD'), 6, 2)  date_m,--月份
       cpxid                                            pro_line_id,--产品线ID
       cpxmc                                            pro_linename,--产品线名称
       cpid                                             pro_id,--产品ID
       cpname                                           pro_name,--产品名称
       case
           when day_num >= 1 and day_num < 15 then '1~15'
           when day_num >= 15 and day_num < 30 then '15~30'
           when day_num >= 30 and day_num < 45 then '30~45'
           when day_num >= 45 then '>=45'
           end                                          bug_days,--bug滞留时长
       count(distinct id) as                            cp_bug_num--bug数量
from (select cpxid,
             cpxmc,
             cpid,
             cpname,
             module_id,
             pro_id,
             b.id,
             b.severity,
             b.product,
             b.status,
             b."activatedCount",
             date("openedDate")                      as event_date,
             date(current_date) - date("openedDate") as day_num
      from (select cpxid,
                   cpxmc,
                   cpid,
                   cpname,
                   module_id,
                   p.id as pro_id
            from (select cpxid, cpxmc, cpid, cpname, m.id as module_id
                  from (select cpxid,
                               cpxmc,
                               cpid,
                               name cpname
                        from ex_ods_pass_ecology_uf_productline as t1
                                 left join ex_ods_pass_ecology_uf_product as t2 on t1.cpxid = t2.linaname
                        where cpxid in ('03', '02', '22','23','24', '05') and cpid not in ('022','030','082','029','028','123','027')
                        ) oa_cp
                           join ods_zentao_zt_module as m on oa_cp.cpid = cast(m.produtctid as text)) as cpm
                     join ods_zentao_zt_product as p on cast(cpm.module_id as text) = cast(p.line as text)
            where p.deleted = '0') as cpmp
               join ex_ods_cd_zentao_zt_bug as b on cpmp.pro_id = b.product
      where b.deleted = '0'
        and b.origin = 'ITR'
        AND b.status != 'closed'
        and date("openedDate") >= date('2022-01-01')) as t
group by substr(to_char(event_date, 'YYYY-MM-DD'), 1, 4),
         EXTRACT('QUARTER' from cast(event_date as date)),
         substr(to_char(event_date, 'YYYY-MM-DD'), 6, 2),
         cpxid,
         cpxmc,
         cpid,
         cpname,
         case
             when day_num >= 1 and day_num < 15 then '1~15'
             when day_num >= 15 and day_num < 30 then '15~30'
             when day_num >= 30 and day_num < 45 then '30~45'
             when day_num >= 45 then '>=45'
             end;
--严重度分布
truncate table ads_bug_level_days;
insert into ads_bug_level_days
select substr(to_char(event_date, 'YYYY-MM-DD'), 1, 4)  date_y,--年份
       EXTRACT('QUARTER' from cast(event_date as date)) date_q,--季度
       substr(to_char(event_date, 'YYYY-MM-DD'), 6, 2)  date_m,--月份
       cpxid                                            pro_line_id,--产品线ID
       cpxmc                                            pro_linename,--产品线名称
       cpid                                             pro_id,--产品ID
       cpname                                           pro_name,--产品名称
       case
           when severity = 1 then '致命'
           when severity = 2 then '严重'
           when severity = 3 then '一般'
           when severity = 4 then '建议'
           end                                          bug_level,--bug严重程度
       count(distinct id) as                            cp_bug_num--bug数量
from (select cpxid,
             cpxmc,
             cpid,
             cpname,
             module_id,
             pro_id,
             b.id,
             b.severity,
             b.product,
             b.status,
             b."activatedCount",
             date("openedDate")                      as event_date,
             date(current_date) - date("openedDate") as day_num
      from (select cpxid,
                   cpxmc,
                   cpid,
                   cpname,
                   module_id,
                   p.id as pro_id
            from (select cpxid, cpxmc, cpid, cpname, m.id as module_id
                  from (select cpxid,
                               cpxmc,
                               cpid,
                               name cpname
                        from ex_ods_pass_ecology_uf_productline as t1
                                 left join ex_ods_pass_ecology_uf_product as t2 on t1.cpxid = t2.linaname
                        where cpxid in ('03', '02', '22','23','24', '05') and cpid not in ('022','030','082','029','028','123','027')
                        ) oa_cp
                           join ods_zentao_zt_module as m on oa_cp.cpid = cast(m.produtctid as text)) as cpm
                     join ods_zentao_zt_product as p on cast(cpm.module_id as text) = cast(p.line as text)
            where p.deleted = '0') as cpmp
               join ex_ods_cd_zentao_zt_bug as b on cpmp.pro_id = b.product
      where b.deleted = '0'
        and b.origin = 'ITR'
        AND b.status != 'closed'
        and date("openedDate") >= date('2022-01-01')) as t
group by substr(to_char(event_date, 'YYYY-MM-DD'), 1, 4),
         EXTRACT('QUARTER' from cast(event_date as date)),
         substr(to_char(event_date, 'YYYY-MM-DD'), 6, 2),
         cpxid,
         cpxmc,
         cpid,
         cpname,
         case
             when severity = 1 then '致命'
             when severity = 2 then '严重'
             when severity = 3 then '一般'
             when severity = 4 then '建议'
             end;

--⑤截至昨日未关闭Bug明细
truncate table ads_bug_opened_table;
insert into ads_bug_opened_table
select substr(to_char(event_date, 'YYYY-MM-DD'), 1, 4)             date_y,--年份
       EXTRACT('QUARTER' from cast(event_date as date))            date_q,--季度
       substr(to_char(event_date, 'YYYY-MM-DD'), 6, 2)             date_m,--月份
       cpxid                                                       pro_line_id,--产品线ID
       cpxmc                                                       pro_linename,--产品线名称
       cpid                                                        pro_id,--产品ID
       cpname                                                      pro_name,--产品名称
       id,
       title                                                       bug_title,--bug标题
       case
           when status = 'active' then '激活'
           when status = 'closed' then '已关闭'
           when status = 'resolved' then '已解决'
           end                                                     bug_status,--状态
       case
           when "subStatus" = '1' then '激活'
           when "subStatus" = '102' then '测试经理审核'
           when "subStatus" = '103' then '研发经理审核'
           when "subStatus" = '104' then '问题定位修改'
           when "subStatus" = '105' then '修改实施审核'
           when "subStatus" = '106' then '问题待决策'
           when "subStatus" = '6' then '已解决待回归'
           when "subStatus" = '601' then '回归测试'
           when "subStatus" = '602' then '问题关闭确认'
           when "subStatus" = '10' then '问题解决关闭'
           when "subStatus" = '1002' then '非问题关闭'
           when "subStatus" = '1003' then '重复问题关闭'
           when "subStatus" = '1004' then '决策不解决关闭'
           when "subStatus" = '1005' then '转需求关闭'
           end                                                     bug_substatus,--子状态
       case
           when severity = 1 then '致命'
           when severity = 2 then '严重'
           when severity = 3 then '一般'
           when severity = 4 then '建议'
           end                                                     bug_level,--bug严重程度
       event_date,--创建日期
       "assignedTo" as assignedto,--当前指派人
       "case"                                                      related, --关联用例
       case
           when day_num >= 1 and day_num < 15 then '1~15'
           when day_num >= 15 and day_num < 30 then '15~30'
           when day_num >= 30 and day_num < 45 then '30~45'
           when day_num >= 45 then '>=45'
           end                                                     bug_days,--bug滞留时长
       substr(COALESCE(("closedDate"::text), '0000-00-00'), 1, 10) closeddate --bug的关闭时间
from (select cpxid,
             cpxmc,
             cpid,
             cpname,
             module_id,
             pro_id,
             b.*,
             date(current_date) - date("openedDate") as day_num,
             date("openedDate")                      as event_date
      from (select cpxid,
                   cpxmc,
                   cpid,
                   cpname,
                   module_id,
                   p.id as pro_id
            from (select cpxid, cpxmc, cpid, cpname, m.id as module_id
                  from (select cpxid,
                               cpxmc,
                               cpid,
                               name cpname
                        from ex_ods_pass_ecology_uf_productline as t1
                                 left join ex_ods_pass_ecology_uf_product as t2
                                           on t1.cpxid = t2.linaname
                        where cpxid in ('03', '02','22','23','24', '05')
                          and cpid not in
                              ('022', '030', '082', '029', '028', '123', '027')) oa_cp
                           join ods_zentao_zt_module as m
                                on oa_cp.cpid = cast(m.produtctid as text)) as cpm
                     join ods_zentao_zt_product as p
                          on cast(cpm.module_id as text) = cast(p.line as text)
            where p.deleted = '0') as cpmp
               join ex_ods_cd_zentao_zt_bug as b on cpmp.pro_id = b.product
      where b.deleted = '0'
        and b.origin = 'ITR'
        -- AND b.status != 'closed'
        and date("openedDate") >= date('2022-01-01')) as t
group by substr(to_char(event_date, 'YYYY-MM-DD'), 1, 4),
         EXTRACT('QUARTER' from cast(event_date as date)),
         substr(to_char(event_date, 'YYYY-MM-DD'), 6, 2),
         cpxid,
         cpxmc,
         cpid,
         cpname,
         id,
         title,
         case
             when status = 'active' then '激活'
             when status = 'closed' then '已关闭'
             when status = 'resolved' then '已解决'
             end,
         case
             when "subStatus" = '1' then '激活'
             when "subStatus" = '102' then '测试经理审核'
             when "subStatus" = '103' then '研发经理审核'
             when "subStatus" = '104' then '问题定位修改'
             when "subStatus" = '105' then '修改实施审核'
             when "subStatus" = '106' then '问题待决策'
             when "subStatus" = '6' then '已解决待回归'
             when "subStatus" = '601' then '回归测试'
             when "subStatus" = '602' then '问题关闭确认'
             when "subStatus" = '10' then '问题解决关闭'
             when "subStatus" = '1002' then '非问题关闭'
             when "subStatus" = '1003' then '重复问题关闭'
             when "subStatus" = '1004' then '决策不解决关闭'
             when "subStatus" = '1005' then '转需求关闭'
             end,
         case
             when severity = 1 then '致命'
             when severity = 2 then '严重'
             when severity = 3 then '一般'
             when severity = 4 then '建议'
             end,
         event_date,
         "assignedTo",
         "case",
         case
             when day_num >= 1 and day_num < 15 then '1~15'
             when day_num >= 15 and day_num < 30 then '15~30'
             when day_num >= 30 and day_num < 45 then '30~45'
             when day_num >= 45 then '>=45'
             end,
         substr(COALESCE(("closedDate"::text), '0000-00-00'), 1, 10);


---4、市场需求RR分析
--①RR需求累积趋势
--RR需求累积趋势-月度-修改后
truncate table ads_rr_demand_qty;
insert into ads_rr_demand_qty
select t1.date_y,--年份
       t1.date_q,--季度
       t1.date_m,--月份
       t1.pro_line_id,--产品线ID
       t1.pro_id,--产品ID
       t1.pro_linename,--产品线名称
       t1.pro_name,--产品名称
       total_num,--总数量
       deald_num,-- 已处理市场需求数
       scheduled_num--已排期市场需求数
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
            where cpxid in ('03', '02', '22','23','24', '05', '10') and cpid not in ('022','030','082','029','028','123','027')
            ) as t1
               cross join (SELECT substr(to_char(dates, 'YYYY-MM-DD'), 1, 4)  AS year,
                                  EXTRACT('QUARTER' from cast(dates as date)) AS quarter,
                                  substr(to_char(dates, 'YYYY-MM-DD'), 6, 2)  AS month
                           FROM (SELECT GENERATE_SERIES('2022-01-01'::DATE, current_date::DATE,
                                                        '1 month'::INTERVAL) AS dates) subquery) as t2) as t1
         left join
     (with temp_t as (select substr(t4.tjsj, 1, 4)                         date_y,--年份
                             EXTRACT('QUARTER' from cast(t4.tjsj as date)) date_q,--季度
                             substr(t4.tjsj, 6, 2) as                      date_m,--月份
                             t4.cpx                                        pro_line_id,--产品线ID
                             t4.cpxcpbd                                    pro_id,--产品ID
                             t2.cpxmc                                      pro_linename,--产品线名称
                             t3.name                                       pro_name,--产品名称
                             count(distinct t4.id)                         total_num--总数量
                      from ex_ods_pass_ecology_uf_IPDxqgl  as t4
                               left join ex_ods_pass_ecology_BusinessLog_view t1 on cast(t4.id as text) = t1.rrxq
                               left join ex_ods_pass_ecology_uf_productline as t2 on t4.cpx = t2.cpxid
                               left join ex_ods_pass_ecology_uf_product as t3 on t4.cpxcpbd = t3.cpid
                      where
                         t4.cpx in ('03', '02', '22','23','24', '05', '10') and t4.cpxcpbd not in ('022','030','082','029','028','123','027')
                      group by substr(t4.tjsj, 1, 4),
                               EXTRACT('QUARTER' from cast(t4.tjsj as date)),
                               substr(t4.tjsj, 6, 2),
                               t4.cpx,
                               t4.cpxcpbd,
                               t2.cpxmc,
                               t3.name)
      SELECT distinct date_y,
                      date_q,
                      date_m,
                      pro_line_id,
                      pro_id,
                      pro_linename,
                      pro_name,
                      sum(total_num)
                      OVER (partition by pro_line_id,pro_id,pro_linename,pro_name ORDER BY date_y,date_q, date_m) AS total_num
      FROM temp_t) as t2
     on t1.date_y = t2.date_y and t1.date_q = t2.date_q and cast(t1.date_m as text) = t2.date_m and
        t1.pro_line_id = t2.pro_line_id and t1.pro_id = t2.pro_id
         left join (with temp_t as (select substr(case when cljg='不接纳' and t4.psslrq is null  then cldate else psslrq end, 1, 4)                         date_y,--年份
                                           EXTRACT('QUARTER' from cast(case when cljg='不接纳' and t4.psslrq is null  then cldate else psslrq end as date)) date_q,--季度
                                           substr(case when cljg='不接纳' and t4.psslrq is null  then cldate else psslrq end, 6, 2)           as            date_m,--月份
                                           t4.cpx                                          pro_line_id,--产品线ID
                                           t4.cpxcpbd                                      pro_id,--产品ID
                                           t2.cpxmc                                        pro_linename,--产品线名称
                                           t3.name                                         pro_name,--产品名称
                                           count(distinct case
                                                              when lx in (1, 5) and xqzt in (1, 2, 3, 4) then t4.id
                                                              else null end) as            deald_num-- 已处理市场需求数
                                    from ex_ods_pass_ecology_uf_IPDxqgl  as t4
                                             left join ex_ods_pass_ecology_BusinessLog_view t1 on cast(t4.id as text) = t1.rrxq
                                             left join ex_ods_pass_ecology_uf_productline as t2 on t4.cpx = t2.cpxid
                                             left join ex_ods_pass_ecology_uf_product as t3 on t4.cpxcpbd = t3.cpid
                                    where t4.cpx in ('03', '02', '22','23','24', '05', '10') and t4.cpxcpbd not in ('022','030','082','029','028','123','027')
                                    group by substr(case when cljg='不接纳' and t4.psslrq is null  then cldate else psslrq end, 1, 4),
                                             EXTRACT('QUARTER' from cast(case when cljg='不接纳' and t4.psslrq is null  then cldate else psslrq end as date)),
                                             substr(case when cljg='不接纳' and t4.psslrq is null  then cldate else psslrq end, 6, 2),
                                             t4.cpx,
                                             t4.cpxcpbd,
                                             t2.cpxmc,
                                             t3.name)
                    SELECT distinct date_y,
                                    date_q,
                                    date_m,
                                    pro_line_id,
                                    pro_id,
                                    pro_linename,
                                    pro_name,
                                    sum(deald_num)
                                    OVER (partition by pro_line_id,pro_id,pro_linename,pro_name ORDER BY date_y,date_q, date_m) AS deald_num
                    FROM temp_t) as t3
                   on t1.date_y = t3.date_y and t1.date_q = t3.date_q and cast(t1.date_m as text) = t3.date_m and
                      t1.pro_line_id = t3.pro_line_id and t1.pro_id = t3.pro_id
         left join
     (with temp_t as (select substr(case
                                        when t5.yjjfrq is not null then (t5.modedatacreatedate)
                                        when t5.yjjfrq is null then (t6.modedatacreatedate)
                                        end, 1, 4)                date_y,--年份
                             EXTRACT('QUARTER' from cast(case
                                                             when t5.yjjfrq is not null then (t5.modedatacreatedate)
                                                             when t5.yjjfrq is null then (t6.modedatacreatedate)
                                 end as date))                    date_q,--季度
                             substr(case
                                        when t5.yjjfrq is not null then (t5.modedatacreatedate)
                                        when t5.yjjfrq is null then (t6.modedatacreatedate)
                                        end, 6, 2)             as date_m,--月份

                             t4.cpx                               pro_line_id,--产品线ID
                             t4.cpxcpbd                           pro_id,--产品ID
                             t2.cpxmc                             pro_linename,--产品线名称
                             t3.name                              pro_name,--产品名称
                             count(distinct case
                                                when lx in (1,2, 3) and t4.pqjf <> 0 then t4.id
                                                else null end) as scheduled_num--已排期市场需求数
                      from ex_ods_pass_ecology_uf_IPDxqgl  as t4
                               left join ex_ods_pass_ecology_BusinessLog_view t1 on cast(t4.id as text) = t1.rrxq
                               left join ex_ods_pass_ecology_uf_ipdxqys t5 on cast(t4.id as text) = t5.rrxq
                               left join ex_ods_pass_ecology_uf_ipdxqpq t6 on cast(t4.id as text) = t6.xqbh
                               left join ex_ods_pass_ecology_uf_productline as t2 on t4.cpx = t2.cpxid
                               left join ex_ods_pass_ecology_uf_product as t3 on t4.cpxcpbd = t3.cpid
                      where  t4.cpx in ('03', '02', '22','23','24', '05', '10') and t4.cpxcpbd not in ('022','030','082','029','028','123','027')
                      group by substr(case
                                          when t5.yjjfrq is not null then (t5.modedatacreatedate)
                                          when t5.yjjfrq is null then (t6.modedatacreatedate)
                                          end, 1, 4),
                               EXTRACT('QUARTER' from cast(case
                                                               when t5.yjjfrq is not null then (t5.modedatacreatedate)
                                                               when t5.yjjfrq is null then (t6.modedatacreatedate)
                                   end as date)),
                               substr(case
                                          when t5.yjjfrq is not null then (t5.modedatacreatedate)
                                          when t5.yjjfrq is null then (t6.modedatacreatedate)
                                          end, 6, 2),
                               t4.cpx,
                               t4.cpxcpbd,
                               t2.cpxmc,
                               t3.name)
      SELECT distinct date_y,
                      date_q,
                      date_m,
                      pro_line_id,
                      pro_id,
                      pro_linename,
                      pro_name,
                      sum(scheduled_num)
                      OVER (partition by pro_line_id,pro_id,pro_linename,pro_name ORDER BY date_y,date_q, date_m) AS scheduled_num
      FROM temp_t) as t4
     on t1.date_y = t4.date_y and t1.date_q = t4.date_q and cast(t1.date_m as text) = t4.date_m and
        t1.pro_line_id = t4.pro_line_id and t1.pro_id = t4.pro_id;


--RR需求累积趋势-季度-新增
truncate table ads_rr_demand_quarterqty;
insert into  ads_rr_demand_quarterqty
select t1.date_y,--年份
       t1.date_q,--季度
       t1.pro_line_id,--产品线ID
       t1.pro_id,--产品ID
       t1.pro_linename,--产品线名称
       t1.pro_name,--产品名称
       total_num,--总数量
       deald_num,-- 已处理市场需求数
       scheduled_num--已排期市场需求数
from (select year    as date_y,
             quarter as date_q,
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
            where cpxid in ('03', '02', '22','23','24', '05', '10') and cpid not in ('022','030','082','029','028','123','027')
            ) as t1
               cross join (SELECT substr(to_char(dates, 'YYYY-MM-DD'), 1, 4)  AS year,
                                  EXTRACT('QUARTER' from cast(dates as date)) AS quarter
                           FROM (SELECT GENERATE_SERIES('2022-01-01'::DATE, current_date::DATE,
                                                        '1 month'::INTERVAL) AS dates) subquery
                           group by substr(to_char(dates, 'YYYY-MM-DD'), 1, 4) ,
                                  EXTRACT('QUARTER' from cast(dates as date))

                           ) as t2) as t1
         left join
     (with temp_t as (select substr(t4.tjsj, 1, 4)                         date_y,--年份
                             EXTRACT('QUARTER' from cast(t4.tjsj as date)) date_q,--季度
                             substr(t4.tjsj, 6, 2) as                      date_m,--月份
                             t4.cpx                                        pro_line_id,--产品线ID
                             t4.cpxcpbd                                    pro_id,--产品ID
                             t2.cpxmc                                      pro_linename,--产品线名称
                             t3.name                                       pro_name,--产品名称
                             count(distinct t4.id)                         total_num--总数量
                      from ex_ods_pass_ecology_uf_IPDxqgl  as t4
                               left join ex_ods_pass_ecology_BusinessLog_view t1 on cast(t4.id as text) = t1.rrxq
                               left join ex_ods_pass_ecology_uf_productline as t2 on t4.cpx = t2.cpxid
                               left join ex_ods_pass_ecology_uf_product as t3 on t4.cpxcpbd = t3.cpid
                      where   t4.cpx in ('03', '02', '22','23','24', '05', '10') and t4.cpxcpbd not in ('022','030','082','029','028','123','027')
                      group by substr(t4.tjsj, 1, 4),
                               EXTRACT('QUARTER' from cast(t4.tjsj as date)),
                               substr(t4.tjsj, 6, 2),
                               t4.cpx,
                               t4.cpxcpbd,
                               t2.cpxmc,
                               t3.name)
      SELECT distinct date_y,
                      date_q,
                      pro_line_id,
                      pro_id,
                      pro_linename,
                      pro_name,
                      sum(total_num)
                      OVER (partition by pro_line_id,pro_id,pro_linename,pro_name ORDER BY date_y,date_q) AS total_num
      FROM temp_t) as t2
     on t1.date_y = t2.date_y and t1.date_q = t2.date_q  and
        t1.pro_line_id = t2.pro_line_id and t1.pro_id = t2.pro_id
         left join (with temp_t as (select substr(case when cljg='不接纳' and t4.psslrq is null  then cldate else psslrq end, 1, 4)                         date_y,--年份
                                           EXTRACT('QUARTER' from cast(case when cljg='不接纳' and t4.psslrq is null  then cldate else psslrq end as date)) date_q,--季度
                                           substr(case when cljg='不接纳' and t4.psslrq is null  then cldate else psslrq end, 6, 2)           as            date_m,--月份
                                           t4.cpx                                          pro_line_id,--产品线ID
                                           t4.cpxcpbd                                      pro_id,--产品ID
                                           t2.cpxmc                                        pro_linename,--产品线名称
                                           t3.name                                         pro_name,--产品名称
                                           count(distinct case
                                                              when lx in (1, 5) and xqzt in (1, 2, 3, 4) then t4.id
                                                              else null end) as            deald_num-- 已处理市场需求数
                                    from ex_ods_pass_ecology_uf_IPDxqgl  as t4
                                             left join ex_ods_pass_ecology_BusinessLog_view t1 on cast(t4.id as text) = t1.rrxq
                                             left join ex_ods_pass_ecology_uf_productline as t2 on t4.cpx = t2.cpxid
                                             left join ex_ods_pass_ecology_uf_product as t3 on t4.cpxcpbd = t3.cpid
                                    where  t4.cpx in ('03', '02', '22','23','24', '05', '10') and t4.cpxcpbd not in ('022','030','082','029','028','123','027')
                                    group by substr(case when cljg='不接纳' and t4.psslrq is null  then cldate else psslrq end, 1, 4),
                                             EXTRACT('QUARTER' from cast(case when cljg='不接纳' and t4.psslrq is null  then cldate else psslrq end as date)),
                                             substr(case when cljg='不接纳' and t4.psslrq is null  then cldate else psslrq end, 6, 2),
                                             t4.cpx,
                                             t4.cpxcpbd,
                                             t2.cpxmc,
                                             t3.name)
                    SELECT distinct date_y,
                                    date_q,
                                    pro_line_id,
                                    pro_id,
                                    pro_linename,
                                    pro_name,
                                    sum(deald_num)
                                    OVER (partition by pro_line_id,pro_id,pro_linename,pro_name ORDER BY date_y,date_q) AS deald_num
                    FROM temp_t) as t3
                   on t1.date_y = t3.date_y and t1.date_q = t3.date_q and
                      t1.pro_line_id = t3.pro_line_id and t1.pro_id = t3.pro_id
         left join
     (with temp_t as (select substr(case
                                        when t5.yjjfrq is not null then (t5.modedatacreatedate)
                                        when t5.yjjfrq is null then (t6.modedatacreatedate)
                                        end, 1, 4)                date_y,--年份
                             EXTRACT('QUARTER' from cast(case
                                                             when t5.yjjfrq is not null then (t5.modedatacreatedate)
                                                             when t5.yjjfrq is null then (t6.modedatacreatedate)
                                 end as date))                    date_q,--季度
                             substr(case
                                        when t5.yjjfrq is not null then (t5.modedatacreatedate)
                                        when t5.yjjfrq is null then (t6.modedatacreatedate)
                                        end, 6, 2)             as date_m,--月份

                             t4.cpx                               pro_line_id,--产品线ID
                             t4.cpxcpbd                           pro_id,--产品ID
                             t2.cpxmc                             pro_linename,--产品线名称
                             t3.name                              pro_name,--产品名称
                             count(distinct case
                                                when lx in (1,2, 3) and t4.pqjf <> 0 then t4.id
                                                else null end) as scheduled_num--已排期市场需求数
                      from ex_ods_pass_ecology_uf_IPDxqgl  as t4
                               left join ex_ods_pass_ecology_BusinessLog_view t1 on cast(t4.id as text) = t1.rrxq
                               left join ex_ods_pass_ecology_uf_ipdxqys t5 on cast(t4.id as text) = t5.rrxq
                               left join ex_ods_pass_ecology_uf_ipdxqpq t6 on cast(t4.id as text) = t6.xqbh
                               left join ex_ods_pass_ecology_uf_productline as t2 on t4.cpx = t2.cpxid
                               left join ex_ods_pass_ecology_uf_product as t3 on t4.cpxcpbd = t3.cpid
                      where t4.cpx in ('03', '02', '22','23','24', '05', '10') and t4.cpxcpbd not in ('022','030','082','029','028','123','027')
                      group by substr(case
                                          when t5.yjjfrq is not null then (t5.modedatacreatedate)
                                          when t5.yjjfrq is null then (t6.modedatacreatedate)
                                          end, 1, 4),
                               EXTRACT('QUARTER' from cast(case
                                                               when t5.yjjfrq is not null then (t5.modedatacreatedate)
                                                               when t5.yjjfrq is null then (t6.modedatacreatedate)
                                   end as date)),
                               substr(case
                                          when t5.yjjfrq is not null then (t5.modedatacreatedate)
                                          when t5.yjjfrq is null then (t6.modedatacreatedate)
                                          end, 6, 2),
                               t4.cpx,
                               t4.cpxcpbd,
                               t2.cpxmc,
                               t3.name)
      SELECT distinct date_y,
                      date_q,
                      pro_line_id,
                      pro_id,
                      pro_linename,
                      pro_name,
                      sum(scheduled_num)
                      OVER (partition by pro_line_id,pro_id,pro_linename,pro_name ORDER BY date_y,date_q) AS scheduled_num
      FROM temp_t) as t4
     on t1.date_y = t4.date_y and t1.date_q = t4.date_q and
        t1.pro_line_id = t4.pro_line_id and t1.pro_id = t4.pro_id;




--②RR需求逾期未处理/未交付趋势
-- 取每个统计周期截止需求状态为“ 未开始、升级评估中”的需求数
-- 非定制销售项目需求要求：7内处理
-- 定制类销售项目需求要求 ：21内处理
-- 非销售类需求要求：14天内处理
-- 逾期未处理市场的需求由需要管理平台定义，逾期状态从需求管理平台获取
-- 取每个统计周期截止需求存在排期交付日期且在排期交付日期内没有交付的需求数，不包含逾期但是交付的需求数
truncate table ads_rr_overdue_qty;
insert into ads_rr_overdue_qty
select t1.date_y,--年份
       t1.date_q,--季度
       t1.date_m,--月份
       t1.pro_line_id,--产品线ID
       t1.pro_id,--产品ID
       t1.pro_linename,--产品线名称
       t1.pro_name,--产品名称
       overdue_deal_num,--   逾期未处理市场需求数,
       overdue_post_num,--     逾期未交付市场需求数,
       null weichuli_num
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
            where cpxid in ('03', '02', '22','23','24', '05', '10') and cpid not in ('022','030','082','029','028','123','027')
            ) as t1
               cross join (SELECT substr(to_char(dates, 'YYYY-MM-DD'), 1, 4)  AS year,
                                  EXTRACT('QUARTER' from cast(dates as date)) AS quarter,
                                  substr(to_char(dates, 'YYYY-MM-DD'), 6, 2)  AS month
                           FROM (SELECT GENERATE_SERIES('2022-04-01'::DATE, '2023-12-31'::DATE,
                                                        '1 month'::INTERVAL) AS dates) subquery) as t2) as t1
         left join (select substr(t1.csrq, 1, 4)                         date_y,--年份
                           EXTRACT('QUARTER' from cast(t1.csrq as date)) date_q,--季度
                           substr(t1.csrq, 6, 2)                         date_m,--月份
                           t1.cpx                                        pro_line_id,--产品线ID
                           t1.cpxcpbd                                    pro_id,--产品ID
                           t2.cpxmc                                      pro_linename,--产品线名称
                           t3.name                                       pro_name,--产品名称
                           count(case
                                     when xqzt in (0, 1) and date(csrq) < date(current_date) then t1.id
                                     else null end)                      overdue_deal_num--   逾期未处理市场需求数,
                    from ex_ods_pass_ecology_uf_IPDxqgl as t1
                             left join ex_ods_pass_ecology_uf_productline as t2 on t1.cpx = t2.cpxid
                             left join ex_ods_pass_ecology_uf_product as t3 on t1.cpxcpbd = t3.cpid
                    where date(t1.csrq) >= date('2022-01-01')
                      and date(t1.csrq) <= date(current_date)
                      and t1.cpx in ('03', '02', '22','23','24', '05', '10') and t1.cpxcpbd not in ('022','030','082','029','028','123','027')
                    group by substr(t1.csrq, 1, 4),
                             EXTRACT('QUARTER' from cast(t1.csrq as date)),
                             substr(t1.csrq, 6, 2),
                             t1.cpx, t1.cpxcpbd, t2.cpxmc, t3.name) as t2
                   on t1.date_y = t2.date_y and t1.date_q = t2.date_q and cast(t1.date_m as text) = t2.date_m and
                      t1.pro_line_id = t2.pro_line_id and t1.pro_id = t2.pro_id
         left join
     (select substr(t1.yjjfrq, 1, 4)                         date_y,--年份
             EXTRACT('QUARTER' from cast(t1.yjjfrq as date)) date_q,--季度
             substr(t1.yjjfrq, 6, 2)                         date_m,--月份
             t1.cpx                                          pro_line_id,--产品线ID
             t1.cpxcpbd                                      pro_id,--产品ID
             t2.cpxmc                                        pro_linename,--产品线名称
             t3.name                                         pro_name,--产品名称
             count(case
                       when pqjf in (1,2) and
                            to_date(SUBSTR(REPLACE(yjjfrq, ' ', ''), 1, 10), 'YYYY-MM-DD') < date(current_date)
                           then t1.id
                       else null end)                        overdue_post_num--     逾期未交付市场需求数,
      from ex_ods_pass_ecology_uf_IPDxqgl as t1
               left join ex_ods_pass_ecology_uf_productline as t2 on t1.cpx = t2.cpxid
               left join ex_ods_pass_ecology_uf_product as t3 on t1.cpxcpbd = t3.cpid
      where to_date(SUBSTR(REPLACE(yjjfrq, ' ', ''), 1, 10), 'YYYY-MM-DD') >= date('2022-01-01')
        and to_date(SUBSTR(REPLACE(yjjfrq, ' ', ''), 1, 10), 'YYYY-MM-DD') <= date(current_date)
        and t1.cpx in ('03', '02', '22','23','24', '05', '10') and t1.cpxcpbd not in ('022','030','082','029','028','123','027')
      group by substr(t1.yjjfrq, 1, 4),
               EXTRACT('QUARTER' from cast(t1.yjjfrq as date)),
               substr(t1.yjjfrq, 6, 2),
               t1.cpx, t1.cpxcpbd, t2.cpxmc, t3.name) as t3
     on t1.date_y = t3.date_y and t1.date_q = t3.date_q and cast(t1.date_m as text) = t3.date_m and
        t1.pro_line_id = t3.pro_line_id and t1.pro_id = t3.pro_id
where overdue_deal_num is not null or overdue_post_num is not  null;


--③市场需求RR分析表格-没用到
truncate table ads_rr_demand_table;
insert into ads_rr_demand_table
select t1.date_y,
       t1.date_q,
       t1.date_m,
       t1.pro_line_id,
       t1.pro_id,
       t1.pro_linename,
       t1.pro_name,
       t1.total_num,
       t1.deald_num,
       t1.scheduled_num,
       t2.overdue_deal_num,
       t2.overdue_post_num
from (select substr(t4.tjsj, 1, 4)                                                                       date_y,--年份
             EXTRACT('QUARTER' from cast(t4.tjsj as date))                                               date_q,--季度
             substr(t4.tjsj, 6, 2)                                                                    as date_m,--月份
             t4.cpx                                                                                      pro_line_id,--产品线ID
             t4.cpxcpbd                                                                                  pro_id,--产品ID
             t2.cpxmc                                                                                    pro_linename,--产品线名称
             t3.name                                                                                     pro_name,--产品名称
             count(distinct t4.id)                                                                       total_num,--总数量
             count(distinct case when lx in (1, 5) and xqzt in (1, 2, 3, 4) then t4.id else null end) as deald_num,-- 已处理市场需求数
             count(distinct case when lx in (2, 3) and pqjf <> 0 then t4.id else null end)            as scheduled_num--已排期市场需求数
      from ex_ods_pass_ecology_BusinessLog_view as t1
               left join ex_ods_pass_ecology_uf_IPDxqgl t4 on cast(t4.id as text) = t1.rrxq
               left join ex_ods_pass_ecology_uf_productline as t2 on t4.cpx = t2.cpxid
               left join ex_ods_pass_ecology_uf_product as t3 on t4.cpxcpbd = t3.cpid
      where date(t4.tjsj) >= date('2022-01-01')
        and date(t4.tjsj) <= date(current_date)
        and t1.clr is not null
        and (t4.cpjl like concat('%', cast(t1.clr as text), '%') or
             t4.reviewer like concat('%', cast(t1.clr as text), '%'))
        and t4.cpx in ('03', '02', '22','23','24', '05', '10') and t4.cpxcpbd not in ('022','030','082','029','028','123','027')
      group by substr(t4.tjsj, 1, 4),
               EXTRACT('QUARTER' from cast(t4.tjsj as date)),
               substr(t4.tjsj, 6, 2),
               t4.cpx,
               t4.cpxcpbd,
               t2.cpxmc,
               t3.name) as t1
         left join (select substr(t1.tjsj, 1, 4)                                            date_y,--年份
                           EXTRACT('QUARTER' from cast(t1.tjsj as date))                    date_q,--季度
                           substr(t1.tjsj, 6, 2)                                            date_m,--月份
                           t1.cpx                                                           pro_line_id,--产品线ID
                           t1.cpxcpbd                                                       pro_id,--产品ID
                           t2.cpxmc                                                         pro_linename,--产品线名称
                           t3.name                                                          pro_name,--产品名称
                           count(case
                                     when xqzt in (0, 1) and date(csrq) < date(current_date) then t1.id
                                     else null end)                                         overdue_deal_num,--   逾期未处理市场需求数,
                           count(case
                                     when pqjf = 1 and date(yjjfrq) < date(current_date) then t1.id
                                     else null end)                                         overdue_post_num,--     逾期未交付市场需求数,
                           count(distinct (CASE WHEN xqzt = 0 THEN t1.id ELSE null END)) AS weichuli_num--未处理需求数量
                    from ex_ods_pass_ecology_uf_IPDxqgl as t1
                             left join ex_ods_pass_ecology_uf_productline as t2 on t1.cpx = t2.cpxid
                             left join ex_ods_pass_ecology_uf_product as t3 on t1.cpxcpbd = t3.cpid
                    where date(t1.tjsj) >= date('2022-01-01')
                      and date(t1.tjsj) <= date(current_date)
                      and t1.cpx in ('03', '02', '22','23','24', '05', '10') and t1.cpxcpbd not in ('022','030','082','029','028','123','027')
                    group by substr(t1.tjsj, 1, 4),
                             EXTRACT('QUARTER' from cast(t1.tjsj as date)),
                             substr(t1.tjsj, 6, 2),
                             t1.cpx, t1.cpxcpbd, t2.cpxmc, t3.name)
    as t2 on t1.date_y = t2.date_y and
             t1.date_q = t2.date_q and
             t1.date_m = t2.date_m and
             t1.pro_line_id = t2.pro_line_id and
             t1.pro_id = t2.pro_id;





--5、产品IR需求分析
--①IR需求累积趋势
-- 1、根据产品基础信息表.产品编码= zt_module.productId 找到产品的zt_module.id
-- 2、根据：zt_module.id = zt.product.line AND zt.product.deleted=0 找到对应产品的需求库的id（可能会有多个）
-- 3、根据：zt_story.product=zt.product.id AND zt_story.deleted=0 AND zt_story.type="requirement" 找到产品的IR
-- 4、根据：zt_plan！是否为空，统计出是规划的IR数和没有规划的IR数
-- 5、在周期截每月/每季度IR都需要存储下规划的IR数和没有规划的IR数
--IR需求累积趋势-月度--修改后
truncate table ads_ir_demand_qty;
insert into ads_ir_demand_qty
select t1.date_y,--年份
       t1.date_q,--季度
       t1.date_m,--月份
       t1.pro_line_id,--产品线ID
       t1.pro_id,--产品ID
       t1.pro_linename,--产品线名称
       t1.pro_name,--产品名称
       coalesce(total_num, 0)                             total_num,--总数量
       coalesce(total_num, 0) - coalesce(planning_num, 0) unplanning_num,--未规划数量
       coalesce(planning_num, 0)                          planning_num --规划数量
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
                     left join ex_ods_pass_ecology_uf_product as t2
                               on t1.cpxid = t2.linaname
            where cpxid in ('03', '02', '22','23','24', '05', '10')
              and cpid not in ('022', '030', '082', '029', '028', '123', '027')) as t1
               cross join (SELECT substr(to_char(dates, 'YYYY-MM-DD'), 1, 4)  AS year,
                                  EXTRACT('QUARTER' from cast(dates as date)) AS quarter,
                                  substr(to_char(dates, 'YYYY-MM-DD'), 6, 2)  AS month
                           FROM (SELECT GENERATE_SERIES('2021-01-01'::DATE,
                                                        current_date::DATE,
                                                        '1 month'::INTERVAL) AS dates) subquery) as t2) as t1
         left join (with temp_t as (select cp.*,
                                           t3.*,
                                           case
                                               when substr(t3.openeddate, 1, 10) = '0000-00-00'
                                                   then null
                                               else date(t3.openeddate) end as event_date
                                    from (select cpxid,
                                                 cpxmc,
                                                 cpid,
                                                 name cpname
                                          from ex_ods_pass_ecology_uf_productline as t1
                                                   left join ex_ods_pass_ecology_uf_product as t2
                                                             on t1.cpxid = t2.linaname
                                          where cpxid in ('03', '02', '22','23','24', '05', '10')
                                            and cpid not in
                                                ('022', '030', '082', '029', '028', '123',
                                                 '027')) cp
                                             left join ods_zentao_zt_module as t1 on t1.produtctid = cp.cpid
                                             left join ods_zentao_zt_product as t2 on t2.line = t1.id
                                             left join ods_zentao_zt_story as t3 on t3.product = t2.id
                                    where t2.deleted = '0'
                                      and t3.deleted = '0'
                                      and t3.type = 'requirement')
                    SELECT substr(to_char(first_day, 'YYYY-MM-DD'), 1, 4)    date_y,--年份
                           EXTRACT('QUARTER' from cast(first_day as date))   date_q,--季度
                           substr(to_char(first_day, 'YYYY-MM-DD'), 6, 2) as date_m,--月份
                           cpxid                                             pro_line_id,--产品线ID
                           cpid                                              pro_id,--产品ID
                           cpxmc                                             pro_linename,--产品线名称
                           cpname                                            pro_name,--产品名称
                           count(distinct id)                             as total_num--总数量
                    FROM temp_t
                             JOIN
                         (SELECT DATE_TRUNC('month', event_date) AS first_day,
                                 MAX(event_date)                 AS last_day
                          FROM temp_t
                          GROUP BY DATE_TRUNC('month', event_date)) AS last_days
                         ON
                             temp_t.event_date <= last_days.last_day
                    GROUP BY substr(to_char(first_day, 'YYYY-MM-DD'), 1, 4),
                             EXTRACT('QUARTER' from cast(first_day as date)),
                             substr(to_char(first_day, 'YYYY-MM-DD'), 6, 2),
                             cpxid,
                             cpid,
                             cpxmc,
                             cpname) as t2
                   on t1.date_y = t2.date_y and t1.date_q = t2.date_q and
                      cast(t1.date_m as text) = t2.date_m and
                      t1.pro_line_id = t2.pro_line_id and t1.pro_id = t2.pro_id
         left join(with temp_t as (select cpxid,
                                          cpxmc,
                                          cpid,
                                          cpname,
                                          plan,
                                          id,
                                          min(event_date) as event_date
                                   from (select cp.cpxid,
                                                cp.cpxmc,
                                                cp.cpid,
                                                cp.cpname,
                                                t3.plan,
                                                t3.id,
                                                date(t4.date) as           action_date,
                                                date(t3.openeddate),
                                                CASE
                                                    WHEN date(t4.date) is null then case
                                                                                        when substr(t3.openeddate, 1, 10) = '0000-00-00'
                                                                                            then null
                                                                                        else date(t3.openeddate) end
                                                    else date(t4.date) end event_date
                                         from (select cpxid,
                                                      cpxmc,
                                                      cpid,
                                                      name cpname
                                               from ex_ods_pass_ecology_uf_productline as t1
                                                        left join ex_ods_pass_ecology_uf_product as t2
                                                                  on t1.cpxid = t2.linaname
                                               where cpxid in ('03', '02', '22','23','24', '05', '10')
                                                 and cpid not in
                                                     ('022', '030', '082', '029', '028',
                                                      '123', '027')) cp
                                                  left join ods_zentao_zt_module as t1 on t1.produtctid = cp.cpid
                                                  left join ods_zentao_zt_product as t2 on t2.line = t1.id
                                                  left join ods_zentao_zt_story as t3 on t3.product = t2.id
                                                  left join
                                              (select t1.objectid, t1.date, t1.id
                                               from ods_zentao_zt_action as t1
                                                        join ods_zentao_zt_history as t2 on t1.id = t2.action
                                               where t1.objectType = 'story'
                                                 AND t1.action in ('linked2plan', 'edited')
                                                 AND t2.field = 'plan') as t4
                                              on t3.id = t4.objectID
                                         where t2.deleted = '0'
                                           and t3.deleted = '0'
                                           and length(t3.plan) <> 0
                                           and t3.type = 'requirement') as t
                                   group by cpxid,
                                            cpxmc,
                                            cpid,
                                            cpname,
                                            plan,
                                            id)
                   SELECT substr(to_char(first_day, 'YYYY-MM-DD'), 1, 4)    date_y,--年份
                          EXTRACT('QUARTER' from cast(first_day as date))   date_q,--季度
                          substr(to_char(first_day, 'YYYY-MM-DD'), 6, 2) as date_m,--月份
                          cpxid                                             pro_line_id,--产品线ID
                          cpid                                              pro_id,--产品ID
                          cpxmc                                             pro_linename,--产品线名称
                          cpname                                            pro_name,--产品名称
                          count(distinct id)                             as planning_num --规划数量
                   FROM temp_t
                            JOIN
                        (SELECT DATE_TRUNC('month', event_date) AS first_day,
                                MAX(event_date)                 AS last_day
                         FROM temp_t
                         GROUP BY DATE_TRUNC('month', event_date)) AS last_days
                        ON
                            temp_t.event_date <= last_days.last_day
                   GROUP BY substr(to_char(first_day, 'YYYY-MM-DD'), 1, 4),
                            EXTRACT('QUARTER' from cast(first_day as date)),
                            substr(to_char(first_day, 'YYYY-MM-DD'), 6, 2),
                            cpxid,
                            cpid,
                            cpxmc,
                            cpname) as t3
                  on t1.date_y = t3.date_y and t1.date_q = t3.date_q and
                     cast(t1.date_m as text) = t3.date_m and
                     t1.pro_line_id = t3.pro_line_id and t1.pro_id = t3.pro_id;



--IR需求累积趋势-季度-新增
truncate table ads_ir_demand_quarterqty;
insert into  ads_ir_demand_quarterqty
select t1.date_y,--年份
       t1.date_q,--季度
       t1.pro_line_id,--产品线ID
       t1.pro_id,--产品ID
       t1.pro_linename,--产品线名称
       t1.pro_name,--产品名称
       coalesce(total_num, 0)                             total_num,--总数量
       coalesce(total_num, 0) - coalesce(planning_num, 0) unplanning_num,--未规划数量
       coalesce(planning_num, 0)                          planning_num --规划数量
from (select year    as date_y,
             quarter as date_q,
             t1.cpxid   pro_line_id,--产品线ID
             t1.cpid    pro_id,--产品ID
             t1.cpxmc   pro_linename,--产品线名称
             t1.cpname  pro_name --产品名称
      from (select cpxid,
                   cpxmc,
                   cpid,
                   name cpname
            from ex_ods_pass_ecology_uf_productline as t1
                     left join ex_ods_pass_ecology_uf_product as t2
                               on t1.cpxid = t2.linaname
            where cpxid in ('03', '02', '22','23','24', '05', '10')
              and cpid not in ('022', '030', '082', '029', '028', '123', '027')) as t1
               cross join (SELECT substr(to_char(dates, 'YYYY-MM-DD'), 1, 4)  AS year,
                                  EXTRACT('QUARTER' from cast(dates as date)) AS quarter
                           FROM (SELECT GENERATE_SERIES('2021-01-01'::DATE,
                                                        current_date::DATE,
                                                        '1 month'::INTERVAL) AS dates) subquery
                           group by substr(to_char(dates, 'YYYY-MM-DD'), 1, 4),
                                    EXTRACT('QUARTER' from cast(dates as date))) as t2) as t1
         left join (with temp_t as (select cp.*,
                                           t3.*,
                                           case
                                               when substr(t3.openeddate, 1, 10) = '0000-00-00'
                                                   then null
                                               else date(t3.openeddate) end as event_date
                                    from (select cpxid,
                                                 cpxmc,
                                                 cpid,
                                                 name cpname
                                          from ex_ods_pass_ecology_uf_productline as t1
                                                   left join ex_ods_pass_ecology_uf_product as t2
                                                             on t1.cpxid = t2.linaname
                                          where cpxid in ('03', '02', '22','23','24', '05', '10')
                                            and cpid not in
                                                ('022', '030', '082', '029', '028', '123',
                                                 '027')) cp
                                             left join ods_zentao_zt_module as t1 on t1.produtctid = cp.cpid
                                             left join ods_zentao_zt_product as t2 on t2.line = t1.id
                                             left join ods_zentao_zt_story as t3 on t3.product = t2.id
                                    where t2.deleted = '0'
                                      and t3.deleted = '0'
                                      and t3.type = 'requirement')
                    SELECT substr(to_char(first_day, 'YYYY-MM-DD'), 1, 4)  date_y,--年份
                           EXTRACT('QUARTER' from cast(first_day as date)) date_q,--季度
                           cpxid                                           pro_line_id,--产品线ID
                           cpid                                            pro_id,--产品ID
                           cpxmc                                           pro_linename,--产品线名称
                           cpname                                          pro_name,--产品名称
                           count(distinct id) as                           total_num--总数量
                    FROM temp_t
                             JOIN
                         (SELECT DATE_TRUNC('month', event_date) AS first_day,
                                 MAX(event_date)                 AS last_day
                          FROM temp_t
                          GROUP BY DATE_TRUNC('month', event_date)) AS last_days
                         ON
                             temp_t.event_date <= last_days.last_day
                    GROUP BY substr(to_char(first_day, 'YYYY-MM-DD'), 1, 4),
                             EXTRACT('QUARTER' from cast(first_day as date)),
                             cpxid,
                             cpid,
                             cpxmc,
                             cpname) as t2
                   on t1.date_y = t2.date_y and t1.date_q = t2.date_q and
                      t1.pro_line_id = t2.pro_line_id and t1.pro_id = t2.pro_id
         left join(with temp_t as (select cpxid,
                                          cpxmc,
                                          cpid,
                                          cpname,
                                          plan,
                                          id,
                                          min(event_date) as event_date
                                   from (select cp.cpxid,
                                                cp.cpxmc,
                                                cp.cpid,
                                                cp.cpname,
                                                t3.plan,
                                                t3.id,
                                                date(t4.date) as           action_date,
                                                date(t3.openeddate),
                                                CASE
                                                    WHEN date(t4.date) is null then (case
                                                                                         when substr(t3.openeddate, 1, 10) = '0000-00-00'
                                                                                             then null
                                                                                         else date(t3.openeddate) end)
                                                    else date(t4.date) end event_date
                                         from (select cpxid,
                                                      cpxmc,
                                                      cpid,
                                                      name cpname
                                               from ex_ods_pass_ecology_uf_productline as t1
                                                        left join ex_ods_pass_ecology_uf_product as t2
                                                                  on t1.cpxid = t2.linaname
                                               where cpxid in ('03', '02', '22','23','24', '05', '10')
                                                 and cpid not in
                                                     ('022', '030', '082', '029', '028',
                                                      '123', '027')) cp
                                                  left join ods_zentao_zt_module as t1 on t1.produtctid = cp.cpid
                                                  left join ods_zentao_zt_product as t2 on t2.line = t1.id
                                                  left join ods_zentao_zt_story as t3 on t3.product = t2.id
                                                  left join
                                              (select t1.objectid, t1.date, t1.id
                                               from ods_zentao_zt_action as t1
                                                        join ods_zentao_zt_history as t2 on t1.id = t2.action
                                               where t1.objectType = 'story'
                                                 AND t1.action in ('linked2plan', 'edited')
                                                 AND t2.field = 'plan') as t4
                                              on t3.id = t4.objectID
                                         where t2.deleted = '0'
                                           and t3.deleted = '0'
                                           and length(t3.plan) <> 0
                                           and t3.type = 'requirement') as t
                                   group by cpxid,
                                            cpxmc,
                                            cpid,
                                            cpname,
                                            plan,
                                            id)
                   SELECT substr(to_char(first_day, 'YYYY-MM-DD'), 1, 4)  date_y,--年份
                          EXTRACT('QUARTER' from cast(first_day as date)) date_q,--季度
                          cpxid                                           pro_line_id,--产品线ID
                          cpid                                            pro_id,--产品ID
                          cpxmc                                           pro_linename,--产品线名称
                          cpname                                          pro_name,--产品名称
                          count(distinct id) as                           planning_num --规划数量
                   FROM temp_t
                            JOIN
                        (SELECT DATE_TRUNC('month', event_date) AS first_day,
                                MAX(event_date)                 AS last_day
                         FROM temp_t
                         GROUP BY DATE_TRUNC('month', event_date)) AS last_days
                        ON
                            temp_t.event_date <= last_days.last_day
                   GROUP BY substr(to_char(first_day, 'YYYY-MM-DD'), 1, 4),
                            EXTRACT('QUARTER' from cast(first_day as date)),
                            cpxid,
                            cpid,
                            cpxmc,
                            cpname) as t3
                  on t1.date_y = t3.date_y and t1.date_q = t3.date_q and
                     t1.pro_line_id = t3.pro_line_id and t1.pro_id = t3.pro_id;

--②新增IR需求来源统计
-- 1、根据产品基础信息表.产品编码= zt_module.productId 找到产品的zt_module.id
-- 2、根据：zt_module.id = zt.product.line AND zt.product.deleted=0 找到对应产品的需求库的id（可能会有多个）
-- 3、根据：zt_story.product=zt.product.id AND zt_story.deleted=0 AND zt_story.type="requirement" 找到产品的IR
-- 4、根据：zt_story.openedDate创建时间是否在统计周期内，确认新增的IR
-- 5、根据：zt_story.source是否为“rmpush”，统计出来自市场的IR数和内部产生的IR数
truncate table ads_ir_source_qty;
insert into ads_ir_source_qty
select substr(to_char(t1.event_date, 'YYYY-MM-DD'), 1, 4)    date_y,--年份
       EXTRACT('QUARTER' from cast(t1.event_date as date))   date_q,--季度
       substr(to_char(t1.event_date, 'YYYY-MM-DD'), 6, 2) as date_m,--月份
       t1.cpxid                                              pro_line_id,--产品线ID
       t1.cpid                                               pro_id,--产品ID
       t1.cpxmc                                           as pro_linename,--产品线名称
       t1.cpname                                          as pro_name,--产品名称
       case
           when t1.source in ('customer', 'market', 'rmpush') then '来自市场'
           when t1.source in
                ('po', 'SE', 'operation', 'operation', 'dev', 'tester', 'bug', 'other',
                 'safe', 'maintenance')
               then '来自内部'
           else '其他' end                                   marketing,--是否来自市场,
       count(distinct t1.id)                              as total_num --总数量
from (select cp.*,
             t3.*,
             case
                 when substr(t3.openeddate, 1, 10) = '0000-00-00' then null
                 else date(t3.openeddate) end as event_date
      from (select cpxid,
                   cpxmc,
                   cpid,
                   name cpname
            from ex_ods_pass_ecology_uf_productline as t1
                     left join ex_ods_pass_ecology_uf_product as t2
                               on t1.cpxid = t2.linaname
            where cpxid in ('03', '02', '22','23','24', '05', '10')
              and cpid not in ('022', '030', '082', '029', '028', '123', '027')) cp
               left join ods_zentao_zt_module as t1 on t1.produtctid = cp.cpid
               left join ods_zentao_zt_product as t2 on t2.line = t1.id
               left join ods_zentao_zt_story as t3 on t3.product = t2.id
      where t2.deleted = '0'
        and t3.deleted = '0'
        and t3.type = 'requirement') as t1
group by substr(to_char(t1.event_date, 'YYYY-MM-DD'), 1, 4),
         EXTRACT('QUARTER' from cast(t1.event_date as date)),
         substr(to_char(t1.event_date, 'YYYY-MM-DD'), 6, 2),
         t1.cpxid,
         t1.cpid,
         t1.cpxmc,
         t1.cpname,
         case
             when t1.source in ('customer', 'market', 'rmpush') then '来自市场'
             when t1.source in
                  ('po', 'SE', 'operation', 'operation', 'dev', 'tester', 'bug', 'other',
                   'safe', 'maintenance')
                 then '来自内部'
             else '其他' end;


--③初始需求IR分析表格-没用到
truncate table ads_ir_demand_table;
insert into ads_ir_demand_table
select substr(to_char(t1.event_date, 'YYYY-MM-DD'), 1, 4)                  date_y,--年份
       EXTRACT('QUARTER' from cast(t1.event_date as date))                 date_q,--季度
       substr(to_char(t1.event_date, 'YYYY-MM-DD'), 6, 2)               as date_m,--月份
       t1.cpxid                                                            pro_line_id,--产品线ID
       t1.cpid                                                             pro_id,--产品ID
       t1.cpxmc                                                            pro_linename,--产品线名称
       t1.cpname                                                           pro_name,--产品名称
       count(distinct t1.id)                                            as total_num,--总数量
       count(distinct case when plan is null then id else null end)        unplanning_num,--未规划数量
       count(distinct case when plan is not null then id else null end) as planning_num,--规划数量
       count(distinct case
                          when t1.source in ('customer', 'market', 'rmpush') then id
                          else null end)                                   marketing_num,--市场数量,
       count(distinct case
                          when t1.source in
                               ('po', 'SE', 'operation', 'operation', 'dev', 'tester',
                                'bug', 'other', 'safe', 'maintenance') then id
                          else null end)                                as inner_num --内部数量
from (select cp.*,
             t3.*,
             case
                 when substr(t3.openeddate, 1, 10) = '0000-00-00' then null
                 else date(t3.openeddate) end as event_date
      from (select cpxid,
                   cpxmc,
                   cpid,
                   name cpname
            from ex_ods_pass_ecology_uf_productline as t1
                     left join ex_ods_pass_ecology_uf_product as t2
                               on t1.cpxid = t2.linaname
            where cpxid in ('03', '02', '22','23','24', '05', '10')
              and cpid not in ('022', '030', '082', '029', '028', '123', '027')) cp
               left join ods_zentao_zt_module as t1 on t1.produtctid = cp.cpid
               left join ods_zentao_zt_product as t2 on t2.line = t1.id
               left join ods_zentao_zt_story as t3 on t3.product = t2.id
      where t2.deleted = '0'
        and t3.deleted = '0'
        and t3.type = 'requirement') as t1
group by substr(to_char(t1.event_date, 'YYYY-MM-DD'), 1, 4),
         EXTRACT('QUARTER' from cast(t1.event_date as date)),
         substr(to_char(t1.event_date, 'YYYY-MM-DD'), 6, 2),
         t1.cpxid,
         t1.cpid,
         t1.cpxmc,
         t1.cpname;

--6、产品用例分析
--①用例累积趋势
-- 1.根据产品基础信息表.产品编码= zt_module.productId 找到产品的zt_module.id
-- 2.根据：zt_module.id = zt.product.line AND zt.product.deleted=0 找到对应产品的用例库的id（可能会有多个）
-- 3.根据：zt_case.product=zt.product.id AND zt_case.deleted=0 找到产品下所有的用例数
-- 4.在周期截每月/每季度IR都需要存储下用例数
--②新增自动化用例统计
-- 1、同上，先计算统计周期截至自动化用例总数
-- 2、（统计周期N截至自动化用例的累计数)-（统计周期N-1截至自动化用例的累计)作为统计周期N内的新增数据
--④产品用例分析表格-①+②+④
--月度累计用例数据-修改后
truncate table ads_test_case_table;
insert into ads_test_case_table
select t1.date_y,--年份
       t1.date_q,--季度
       t1.date_m,--月份
       t1.pro_line_id,--产品线ID
       t1.pro_linename,--产品线名称
       t1.pro_id,--产品ID
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
            where cpxid in ('03', '02', '22','23','24', '05', '10') --and cpid not in ('022','030','082','029','028','123','027')
            ) as t1
               cross join (SELECT substr(to_char(dates, 'YYYY-MM-DD'), 1, 4)  AS year,
                                  EXTRACT('QUARTER' from cast(dates as date)) AS quarter,
                                  substr(to_char(dates, 'YYYY-MM-DD'), 6, 2)  AS month
                           FROM (SELECT GENERATE_SERIES('2022-01-01'::DATE, current_date::DATE,
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
                                                where cpxid in ('03', '02', '22','23','24', '05', '10') --and cpid not in ('022','030','082','029','028','123','027')
                                                ) cp
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
                   on t1.date_y = t2.date_y and t1.date_q = t2.date_q and cast(t1.date_m as text) = t2.date_m and
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
                                                           WHEN date(t4.date) is null then date(t3.openeddate)
                                                           else date(t4.date) end event_date
                                                from (select cpxid,
                                                             cpxmc,
                                                             cpid,
                                                             name cpname
                                                      from ex_ods_pass_ecology_uf_productline as t1
                                                               left join ex_ods_pass_ecology_uf_product as t2 on t1.cpxid = t2.linaname
                                                      where cpxid in ('03', '02', '22','23','24', '05', '10') --and cpid not in ('022','030','082','029','028','123','027')
                                                      ) cp
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
                                                        AND t2.field = 'SFZDH') as t4 on t3.id = t4.objectID
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
                   on t1.date_y = t3.date_y and t1.date_q = t3.date_q and cast(t1.date_m as text) = t3.date_m and
                      t1.pro_line_id = t3.pro_line_id and t1.pro_id = t3.pro_id;


--季度：新增自动化用例数表①+②+④
--季度累计用例数据-修改后
truncate table ads_test_case_quartertable;
insert into ads_test_case_quartertable
select t1.date_y,--年份
       t1.date_q,--季度
       t1.pro_line_id,--产品线ID
       t1.pro_linename,--产品线名称
       t1.pro_id,--产品ID
       t1.pro_name,--产品名称
       case_num,-- 用例总数
       new_case_num,--新增用例总数
       auto_num,-- 自动化用例数
       new_auto_num --自动化用例新增数
from (select year    as date_y,
             quarter as date_q,
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
            where cpxid in ('03', '02', '22','23','24', '05', '10') and cpid not in ('022','030','082','029','028','123','027')
            ) as t1
               cross join (SELECT substr(to_char(dates, 'YYYY-MM-DD'), 1, 4)  AS year,
                                  EXTRACT('QUARTER' from cast(dates as date)) AS quarter
                           FROM (SELECT GENERATE_SERIES('2022-01-01'::DATE, current_date::DATE,
                                                        '1 month'::INTERVAL) AS dates) subquery
                           group by substr(to_char(dates, 'YYYY-MM-DD'), 1, 4)  ,
                                  EXTRACT('QUARTER' from cast(dates as date))
                           ) as t2) as t1
         left join (with temp_t as (select substr(to_char(t1.event_date, 'YYYY-MM-DD'), 1, 4)  date_y,
                                           EXTRACT('QUARTER' from cast(t1.event_date as date)) date_q,
                                           t1.cpxid                                            pro_line_id,
                                           t1.cpid                                             pro_id,
                                           t1.cpxmc  as                                        pro_linename,
                                           t1.cpname as                                        pro_name,
                                           count(distinct t1.id)                               case_num-- 用例总数
                                    from (select cp.*, t3.*, date(t3.openeddate) as event_date
                                          from (select cpxid,
                                                       cpxmc,
                                                       cpid,
                                                       name cpname
                                                from ex_ods_pass_ecology_uf_productline as t1
                                                         left join ex_ods_pass_ecology_uf_product as t2 on t1.cpxid = t2.linaname
                                                where cpxid in ('03', '02', '22','23','24', '05', '10') and cpid not in ('022','030','082','029','028','123','027')
                                                ) cp
                                                   left join ods_zentao_zt_module as t1 on t1.produtctid = cp.cpid
                                                   left join ods_zentao_zt_product as t2 on t2.line = t1.id
                                                   left join ods_zentao_zt_case as t3 on t3.product = t2.id
                                          where t2.deleted = '0'
                                            and t3.deleted = '0') as t1
                                    group by substr(to_char(t1.event_date, 'YYYY-MM-DD'), 1, 4),
                                             EXTRACT('QUARTER' from cast(t1.event_date as date)),
                                             t1.cpxid,
                                             t1.cpid,
                                             t1.cpxmc,
                                             t1.cpname)
                    SELECT distinct date_y,
                                    date_q,
                                    pro_line_id,
                                    pro_id,
                                    pro_linename,
                                    pro_name,
                                    case_num                                                                               new_case_num,
                                    sum(case_num)
                                    OVER (partition by pro_line_id,pro_id,pro_linename,pro_name ORDER BY date_y,date_q) AS case_num
                    FROM temp_t) as t2
                   on t1.date_y = t2.date_y and t1.date_q = t2.date_q and
                      t1.pro_line_id = t2.pro_line_id and t1.pro_id = t2.pro_id
         left join (with temp_t as (select substr(to_char(t1.event_date, 'YYYY-MM-DD'), 1, 4)              date_y,
                                           EXTRACT('QUARTER' from cast(t1.event_date as date))             date_q,
                                           t1.cpxid                                                        pro_line_id,
                                           t1.cpid                                                         pro_id,
                                           t1.cpxmc  as                                                    pro_linename,
                                           t1.cpname as                                                    pro_name,
                                           count(distinct  t1.id) auto_num-- 自动化用例数
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
                                                           WHEN date(t4.date) is null then date(t3.openeddate)
                                                           else date(t4.date) end event_date
                                                from (select cpxid,
                                                             cpxmc,
                                                             cpid,
                                                             name cpname
                                                      from ex_ods_pass_ecology_uf_productline as t1
                                                               left join ex_ods_pass_ecology_uf_product as t2 on t1.cpxid = t2.linaname
                                                      where cpxid in ('03', '02', '22','23','24', '05', '10') and cpid not in ('022','030','082','029','028','123','027')
                                                      ) cp
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
                                                        AND t2.field = 'SFZDH') as t4 on t3.id = t4.objectID
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
                                             t1.cpxid,
                                             t1.cpid,
                                             t1.cpxmc,
                                             t1.cpname)
                    SELECT distinct date_y,
                                    date_q,
                                    pro_line_id,
                                    pro_id,
                                    pro_linename,
                                    pro_name,
                                    auto_num                                                                               new_auto_num,
                                    sum(auto_num)
                                    OVER (partition by pro_line_id,pro_id,pro_linename,pro_name ORDER BY date_y,date_q) AS auto_num
                    FROM temp_t) as t3
                   on t1.date_y = t3.date_y and t1.date_q = t3.date_q and
                      t1.pro_line_id = t3.pro_line_id and t1.pro_id = t3.pro_id;





--③新增用例统计-是否关联
-- 1.根据产品基础信息表.产品编码= zt_module.productId 找到产品的zt_module.id
-- 2.根据：zt_module.id = zt.product.line AND zt.product.deleted=0 找到对应产品的用例库的id（可能会有多个）
-- 3.根据：zt_case.product=zt.product.id AND zt_case.deleted=0 找到产品下所有的用例
-- 4.根据：zt_case.openedDate创建时间是否在统计周期内，确认新增的用例
-- 5.根据：zt_case.story字段是否为0判断用例是否关联需求，等于“0”是未关联，不等于“0”是关联了需求
truncate table ads_case_related_qty;
insert into ads_case_related_qty
select substr(to_char(t1.event_date, 'YYYY-MM-DD'), 1, 4)    date_y,--年份
       EXTRACT('QUARTER' from cast(t1.event_date as date))   date_q,--季度
       substr(to_char(t1.event_date, 'YYYY-MM-DD'), 6, 2) as date_m,--月份
       t1.cpxid                                              pro_line_id,--产品线ID
       t1.cpid                                               pro_id,--产品ID
       t1.cpxmc                                              pro_linename,--产品线名称
       t1.cpname                                             pro_name,--产品名称
       case when story = 0 or story is null then '未关联' else '关联' end     related, --是否关联SR的用例,
       count(distinct t1.id)                                 case_num --用例总数
from (select cp.*, t3.*, date(t3.openeddate) as event_date
      from (select cpxid,
                                   cpxmc,
                                   cpid,
                                   name cpname
                            from ex_ods_pass_ecology_uf_productline as t1
                                     left join ex_ods_pass_ecology_uf_product as t2 on t1.cpxid = t2.linaname
                            where cpxid in ('03', '02', '22','23','24', '05','10') and cpid not in ('022','030','082','029','028','123','027')
                            ) cp
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
         t1.cpname,
         case when story = 0 or story is null then '未关联' else '关联' end;


---7、产品测试分析
--①+②提测轮次/提测不通过统计  --转测不通过是0
-- 1.根据产品基础信息表.产品编码= zt_module.productId 找到产品的zt_module.id
-- 2.根据：zt_module.id = zt.product.line AND zt.product.deleted=0 找到对应产品的用例库的id（可能会有多个）
-- 3.根据：zt_testtask.product=zt.product.id AND zt_testtask.deleted=0 AND zt_testtask.status=“blocked” 找到产品下转测不通过的测试单
-- 4.在周期截每月/每季度IR都需要存储下转测不同的测试单累计值
-- 5.根据：（统计周期N截至转测不通过累计数)-（统计周期N-1截至转测不通过累计数)作为统计周期N内的新增数据

-- 1.根据产品基础信息表.产品编码= zt_module.productId 找到产品的zt_module.id
-- 2.根据：zt_module.id = zt.product.line AND zt.product.deleted=0 找到对应产品的用例库的id（可能会有多个）
-- 3.根据：zt_testtask.product=zt.product.id AND zt_testtask.deleted=0 找到产品下转测的单子
-- 4.根据： zt_testtask.createdDate 创建时间是否在统计周期内，确认新增的测试单
truncate table ads_test_round_qty;
insert into ads_test_round_qty
with temp_t
         as (select substr(to_char(t1.event_date, 'YYYY-MM-DD'), 1, 4)                    date_y,
                    EXTRACT('QUARTER' from cast(t1.event_date as date))                   date_q,
                    substr(to_char(t1.event_date, 'YYYY-MM-DD'), 6, 2)                 as date_m,
                    cpxid,
                    cpxmc,
                    cpid,
                    cpname,
                    count(distinct case when status = 'blocked' then id else null end) as tice_butongguo_num, --版本提测不通过次数
                    count(distinct id)                                                 as tice_num            --版本提测轮次
             from (select cp.*, t3.*, to_date(t3.createddate,'YYYY-MM-DD')  as event_date
                   from (select cpxid,
                                cpxmc,
                                cpid,
                                name cpname
                         from ex_ods_pass_ecology_uf_productline as t1
                                  left join ex_ods_pass_ecology_uf_product as t2 on t1.cpxid = t2.linaname
                         where cpxid in ('03', '02', '22','23','24', '05','10') and cpid not in ('022','030','082','029','028','123','027')
                         ) cp
                            left join ods_zentao_zt_module as t1 on t1.produtctid = cp.cpid
                            left join ods_zentao_zt_product as t2 on t2.line = t1.id
                            left join ods_zentao_zt_testtask as t3 on t3.product = t2.id
                   where t2.deleted = '0'
                     and t3.deleted = '0') as t1
             group by substr(to_char(t1.event_date, 'YYYY-MM-DD'), 1, 4),
                      EXTRACT('QUARTER' from cast(t1.event_date as date)),
                      substr(to_char(t1.event_date, 'YYYY-MM-DD'), 6, 2),
                      cpxid,
                      cpxmc,
                      cpid,
                      cpname)
select t1.date_y,--年份
       t1.date_q,--季度
       t1.date_m,--月份
       t1.cpxid                                                                   pro_line_id,--产品线ID
       t1.cpxmc                                                                   pro_linename,--产品线名称
       t1.cpid                                                                    pro_id,--产品ID
       t1.cpname                                                                  pro_name,--产品名称
       t1.tice_num                                                             as test_num, --提测轮次数量
       coalesce(t1.tice_butongguo_num, 0) - coalesce(t2.tice_butongguo_num, 0) as test_failed_num --转测不通过数量
from temp_t as t1
         left join temp_t as t2 on
            t1.date_y = t2.date_y
        and t1.cpxid = t2.cpxid
        and t1.cpid = t2.cpid
        and cast(t1.date_m as int) = cast(t2.date_m as int) + 1;


--季度：提测不通过统计
truncate table ads_test_round_quarterqty;
insert into   ads_test_round_quarterqty
with temp_t
         as (select substr(to_char(t1.event_date, 'YYYY-MM-DD'), 1, 4)                    date_y,
                    EXTRACT('QUARTER' from cast(t1.event_date as date))                   date_q,
                    cpxid,
                    cpxmc,
                    cpid,
                    cpname,
                    count(distinct case when status = 'blocked' then id else null end) as tice_butongguo_num, --版本提测不通过次数
                    count(distinct id)                                                 as tice_num            --版本提测轮次
             from (select cp.*, t3.*, to_date(t3.createddate,'YYYY-MM-DD')as event_date
                   from (select cpxid,
                                cpxmc,
                                cpid,
                                name cpname
                         from ex_ods_pass_ecology_uf_productline as t1
                                  left join ex_ods_pass_ecology_uf_product as t2 on t1.cpxid = t2.linaname
                         where cpxid in ('03', '02', '22','23','24', '05','10') and cpid not in ('022','030','082','029','028','123','027')
                         ) cp
                            left join ods_zentao_zt_module as t1 on t1.produtctid = cp.cpid
                            left join ods_zentao_zt_product as t2 on t2.line = t1.id
                            left join ods_zentao_zt_testtask as t3 on t3.product = t2.id
                   where t2.deleted = '0'
                     and t3.deleted = '0') as t1
             group by substr(to_char(t1.event_date, 'YYYY-MM-DD'), 1, 4),
                      EXTRACT('QUARTER' from cast(t1.event_date as date)),
                      cpxid,
                      cpxmc,
                      cpid,
                      cpname)
select t1.date_y,--年份
       t1.date_q,--季度
       t1.cpxid                                                                   pro_line_id,--产品线ID
       t1.cpxmc                                                                   pro_linename,--产品线名称
       t1.cpid                                                                    pro_id,--产品ID
       t1.cpname                                                                  pro_name,--产品名称
       t1.tice_num                                                             as test_num, --提测轮次数量
       coalesce(t1.tice_butongguo_num, 0) - coalesce(t2.tice_butongguo_num, 0) as test_failed_num --转测不通过数量
from temp_t as t1
         left join temp_t as t2 on
            t1.date_y = t2.date_y
        and t1.cpxid = t2.cpxid
        and t1.cpid = t2.cpid
        and cast(t1.date_q as int) = cast(t2.date_q as int) + 1;

--③产品提测分析表格
-- 1.根据产品基础信息表.产品编码= zt_module.productId 找到产品的zt_module.id
-- 2.根据：zt_module.id = zt.product.line AND zt.product.deleted=0 找到对应产品的用例库的id（可能会有多个）
-- 3.根据：zt_testtask.product=zt.product.id AND zt_testtask.deleted=0 AND zt_testtask.status=“blocked” 找到产品下转测不通过的测试单
-- 4.在周期截每月/每季度IR都需要存储下转测不同的测试单累计值
-- 5.根据：（统计周期N截至转测不通过累计数)-（统计周期N-1截至转测不通过累计数)作为统计周期N内的新增数据
--
-- 找转测不通过日期：
-- 1-3同上
-- 4、zt_testtask.status="blocked"，找到转测不通过测试单id，即zt_testtask.id
-- 5、zt_testtask.id=zt_action.objectID,AND zt_action.objectType="testtask" AND zt_action.action="edited",找到这些转测不通过测试单的编辑记录的zt_action.id，但是编辑行为有多种，因此记录会存在多条，
-- 6、zt_action.id=zt.history.action,AND zt_history.new="blocked",找到编辑了“转测不通过”行为的zt_history.action
-- 7、zt_history.action=zt_action.id，找到这条测试单编辑转测不通过的时间记录，即zt_action.date，可能会有多个时间，都记录下来
truncate table ads_test_round_table;
insert into ads_test_round_table
with temp_t as (select substr(to_char(t1.event_date, 'YYYY-MM-DD'), 1, 4)    date_y,--年份
                       EXTRACT('QUARTER' from cast(t1.event_date as date))   date_q,--季度
                       substr(to_char(t1.event_date, 'YYYY-MM-DD'), 6, 2) as date_m,--月份
                       cpxid                                                 pro_line_id,--产品线ID
                       cpxmc                                                 pro_linename,--产品线名称
                       cpid                                                  pro_id,--产品ID
                       cpname                                                pro_name,--产品名称
                       project, --关联项目ID
                       name                                                  test_name,-- 测试单名称,
                       version_nmae                                          test_version,-- 测试版本,
                       event_date,-- 创建日期,
                       realname                                              test_owner,-- 测试负责人,
                       date(begin)                                           plan_begin_date,-- 计划开始,
                       date("end")                                           plan_end_date,-- 计划完成,
                       substr(realfinisheddate, 1, 10)                       act_date,-- 实际完成,
                       btg_date                                              test_failed_date,-- 转测不通过日期,
                       case
                           when status = 'blocked' then '转测不通过'
                           when status = 'doing' then '进行中'
                           when status = 'wait' then '未开始'
                           when status = 'done' then '测试完成'
                           end                                               test_result -- 提测结果
                from (select cp.*,
                             t3.*,
                             to_date(t3.createddate,'YYYY-MM-DD')  as event_date,
                             t4.date              as btg_date,
                             t5.name              as version_nmae,
                             t6.realname
                      from (select cpxid,
                                   cpxmc,
                                   cpid,
                                   name cpname
                            from ex_ods_pass_ecology_uf_productline as t1
                                     left join ex_ods_pass_ecology_uf_product as t2 on t1.cpxid = t2.linaname
                            where cpxid in ('03', '02', '22','23','24', '05', '10') and cpid not in ('022','030','082','029','028','123','027')
                            ) cp
                               left join ods_zentao_zt_module as t1 on t1.produtctid = cp.cpid
                               left join ods_zentao_zt_product as t2 on t2.line = t1.id
                               left join ods_zentao_zt_testtask as t3 on t3.product = t2.id
                               left join (select t1.objectid, t1.date
                                          from ods_zentao_zt_action as t1
                                                   join ods_zentao_zt_history as t2 on t1.id = t2.action
                                                    join ods_zentao_zt_testtask as t3 on t1.objectid=t3.id
                                          where t1.objectType = 'testtask'
                                            and t3.status = 'blocked'
                                            AND t1.action = 'edited'
                                            AND t2.new = 'blocked') as t4 on t4.objectid = t3.id
                               left join ods_zentao_zt_build as t5 on t3.build = cast(t5.id as text)
                               left join ods_zentao_zt_user as t6 on t3.owner = t6.account
                      where t2.deleted = '0'
                        and t3.deleted = '0') as t1
                group by substr(to_char(t1.event_date, 'YYYY-MM-DD'), 1, 4),
                         EXTRACT('QUARTER' from cast(t1.event_date as date)),
                         substr(to_char(t1.event_date, 'YYYY-MM-DD'), 6, 2),
                         cpxid,
                         cpxmc,
                         cpid,
                         cpname,
                         project,
                         name,
                         version_nmae,
                         event_date,
                         realname,
                         date(begin),
                         date("end"),
                         substr(realfinisheddate, 1, 10),
                         btg_date,
                         case
                             when status = 'blocked' then '转测不通过'
                             when status = 'doing' then '进行中'
                             when status = 'wait' then '未开始'
                             when status = 'done' then '测试完成'
                             end)
select date_y,--年份
       date_q,--季度
       date_m,--月份
       pro_line_id,--产品线ID
       pro_linename,--产品线名称
       pro_id,--产品ID
       pro_name,--产品名称
       test_name,-- 测试单名称,
       test_version,-- 测试版本,
       event_date,-- 创建日期,
       test_owner,-- 测试负责人,
       plan_begin_date,-- 计划开始,
       plan_end_date,-- 计划完成,
       act_date,-- 实际完成,
       string_agg((to_char(test_failed_date,'YYYY-MM-DD')::text), ',') test_failed_date,-- 转测不通过日期,
       test_result,-- 提测结果
       project as                                               id --关联项目ID
from temp_t as t
GROUP BY date_y,
         date_q,
         date_m,
         pro_line_id,
         pro_linename,
         pro_id,
         pro_name,
         test_name,
         test_version,
         event_date,
         test_owner,
         plan_begin_date,
         plan_end_date,
         act_date,
         test_result, project;


