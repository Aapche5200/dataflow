--版本维度bug数相关需求
-- 版本的bug
-- 1.根据产品基础信息表.产品编码= zt_module.productId 找到产品的zt_module.id
-- 2.根据：zt_module.id = zt.product.line AND zt.product.deleted=0 找到对应产品的库的id（可能会有多个）
-- 3.根据：zt_productplan.product =  zt.product.id AND zt_productplan.deleted=0 找到产品所有的版本zt_productplan.id
--逻辑有问题，为空 4.产品版本zt_productplan.id=zt_projectproduct.plan ,被归属哪个项目或执行，判断版本bug的统计方式：瀑布和敏捷bug的区分
--   若：zt_projectproduct.project =zt_project.id AND zt_project.module="waterfall"，找到能匹配上的zt_project.id
--   则：zt_bug.project=zt_project.id AND zt_bug.deleted=0  找打产品版本的bug
-- 	若：zt_projectproduct.project =zt_project.id AND zt_project.type="sprint"，找到匹配版本的zt_project.id
-- 	则：zt_bug.execution=zt_project.id AND  zt_bug.deleted=0 找打产品版本的bug
select event_date,
       cpxid,
       cpxmc,
       cpid,
       cpname,
       title,
       sum(cp_bug_num) as spc_bug_num ,-- 产品版本bug 数
    sum(unre_bug_nuim) spc_unre_bug_nuim ,--产品版本未解决的bug数
    sum(second_active_bug_nuim) spc_second_active_bug_nuim --产品版本二次激活bug数
from (select event_date,
             cpxid,
             cpxmc,
             cpid,
             cpname,
             title,
             count(distinct id)                                        as cp_bug_num,
             count(case when status = 'closed' then id else null end)  as unre_bug_nuim,         --产品版本未解决的bug数
             count(case when activatedcount > 0 then id else null end) as second_active_bug_nuim --产品版本二次激活bug数
      from (select cpxid,
                   cpxmc,
                   cpid,
                   cpname,
                   module_id,
                   pro_id,
                   plid,
                   cpmplpjj.title,
                   project_id,
                   p_id,
                   b.id,
                   b.status,
                   b.activatedcount,
                   date(b.openeddate) as event_date
            from (select cpxid,
                         cpxmc,
                         cpid,
                         cpname,
                         module_id,
                         pro_id,
                         plid,
                         title,
                         project_id,
                         j.id as p_id
                  from (select cpxid,
                               cpxmc,
                               cpid,
                               cpname,
                               module_id,
                               pro_id,
                               plid,
                               title,
                               pj.project as project_id
                        from (select cpxid,
                                     cpxmc,
                                     cpid,
                                     cpname,
                                     module_id,
                                     pro_id,
                                     pl.id as plid,
                                     pl.title
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
                                                where cpxid in ('03', '02', '04', '05')) oa_cp
                                                   join ods_zentao_zt_module as m on oa_cp.cpid = cast(m.produtctid as text)) as cpm
                                             join ods_zentao_zt_product as p
                                                  on cast(cpm.module_id as text) = cast(p.line as text)
                                    where p.deleted = '0') cpmp
                                       join ods_zentao_zt_productplan as pl on cpmp.pro_id = pl.product
                              where pl.deleted = '0') as cpmpl
                                 join ((select case
                                                   when length(plan) - length(replace(plan, ',', '')) = 2
                                                       then split_part(plan, ',', 2)
                                                   when length(plan) - length(replace(plan, ',', '')) = 3
                                                       then split_part(plan, ',', 3)
                                                   else plan end plan,
                                               project
                                        from ods_zentao_zt_projectproduct)) as pj
                                      on cast(cpmpl.plid as text) = pj.plan) as cpmplpj
                           join ods_zentao_zt_project as j on cpmplpj.project_id = j.id
                  where j.model = 'waterfall') cpmplpjj
                     join ods_zentao_zt_bug as b on cpmplpjj.p_id = b.project
            where b.deleted = '0') as t
      group by event_date,
               cpxid,
               cpxmc,
               cpid,
               cpname,
               title

      union all
      select event_date,
             cpxid,
             cpxmc,
             cpid,
             cpname,
             title,
             count(distinct id) as cp_bug_num,
             count(case when status = 'closed' then id else null end)  as unre_bug_nuim, --产品版本未解决的bug数
            count(case when activatedcount > 0 then id else null end) as second_active_bug_nuim --产品版本二次激活bug数
      from (select cpxid,
                   cpxmc,
                   cpid,
                   cpname,
                   module_id,
                   pro_id,
                   plid,
                   cpmplpjj.title,
                   project_id,
                   p_id,
                   b.id,
             b.status,
             b.activatedcount,
                   date(b.openeddate) as event_date
            from (select cpxid,
                         cpxmc,
                         cpid,
                         cpname,
                         module_id,
                         pro_id,
                         plid,
                         title,
                         project_id,
                         j.id as p_id
                  from (select cpxid,
                               cpxmc,
                               cpid,
                               cpname,
                               module_id,
                               pro_id,
                               plid,
                               title,
                               pj.project as project_id
                        from (select cpxid,
                                     cpxmc,
                                     cpid,
                                     cpname,
                                     module_id,
                                     pro_id,
                                     pl.id as plid,
                                     pl.title
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
                                                where cpxid in ('03', '02', '04', '05')) oa_cp
                                                   join ods_zentao_zt_module as m on oa_cp.cpid = cast(m.produtctid as text)) as cpm
                                             join ods_zentao_zt_product as p
                                                  on cast(cpm.module_id as text) = cast(p.line as text)
                                    where p.deleted = '0') cpmp
                                       join ods_zentao_zt_productplan as pl on cpmp.pro_id = pl.product
                              where pl.deleted = '0') as cpmpl
                                 join (select case
                                           when length(plan) - length(replace(plan, ',', '')) = 2
                                               then split_part(plan, ',', 2)
                                           when length(plan) - length(replace(plan, ',', '')) = 3
                                               then split_part(plan, ',', 3)
                                           else plan end plan,
                                       project
                                from ods_zentao_zt_projectproduct) as pj on cast (cpmpl.plid as text) = pj.plan) as cpmplpj
                           join ods_zentao_zt_project as j on cpmplpj.project_id = j.id
                  where j.type = 'sprint') cpmplpjj
                     join ods_zentao_zt_bug as b on cpmplpjj.p_id = b.execution
            where b.deleted = '0') as t
      group by event_date,
               cpxid,
               cpxmc,
               cpid,
               cpname, title) as tt
group by event_date,
         cpxid,
         cpxmc,
         cpid,
         cpname,
         title

--另外的写法--按天
select event_date,
       cpxid,
       cpxmc,
       cpid,
       cpname,
       spc_name,
       sum(bug_num) spc_bug_num --版本bug总数
from (select cpxid,
             cpxmc,
             cpid,
             cpname,
             spc_name,
             date(openeddate)   as event_date,
             count(distinct id) as bug_num
      from (select cp.*, t3.title as spc_name, t6.*
            from (select cpxid,
                         cpxmc,
                         cpid,
                         name cpname
                  from ex_ods_pass_ecology_uf_productline as t1
                           left join ex_ods_pass_ecology_uf_product as t2 on t1.cpxid = t2.linaname
                  where cpxid in ('03', '02', '04', '05')) cp
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
                     left join ods_zentao_zt_bug as t6 on t6.project = t5.id
            where t2.deleted = '0'
              and t3.deleted = '0'
              and t5.model = 'waterfall'
              and t6.deleted = '0') as t
      group by cpxid,
               cpxmc,
               cpid,
               cpname, spc_name, date(openeddate)

      union all

      select cpxid,
             cpxmc,
             cpid,
             cpname,
             spc_name,
             date(openeddate)   as event_date,
             count(distinct id) as bug_num
      from (select cp.*, t3.title as spc_name, t6.*
            from (select cpxid,
                         cpxmc,
                         cpid,
                         name cpname
                  from ex_ods_pass_ecology_uf_productline as t1
                           left join ex_ods_pass_ecology_uf_product as t2 on t1.cpxid = t2.linaname
                  where cpxid in ('03', '02', '04', '05')) cp
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
                     left join ods_zentao_zt_bug as t6 on t6.execution = t5.id
            where t2.deleted = '0'
              and t3.deleted = '0'
              and t5.type = 'sprint'
              and t6.deleted = '0') as t
      group by cpxid,
               cpxmc,
               cpid,
               cpname, spc_name, date(openeddate)) as tt
group by event_date,
         cpxid,
         cpxmc,
         cpid,
         cpname,
         spc_name


--版本未解决BUG数
select cpxid,
       cpxmc,
       cpid,
       cpname,
       spc_name,
       sum(spc_unre_bug_num)           spc_unre_bug_num,          --版本未解决BUG数
       sum(spc_second_active_bug_nuim) spc_second_active_bug_nuim,--版本二次激活BUG数
       sum(spc_owner_bug_nuim)         spc_owner_bug_nuim--提交的BUG数
from (select cpxid,
             cpxmc,
             cpid,
             cpname,
             spc_name,
             count(distinct case when status != 'closed' then id else null end)   as spc_unre_bug_num,
             count(distinct case when activatedcount > 0 then id else null end)   as spc_second_active_bug_nuim,
             count(distinct case when openedby is not null then id else null end) as spc_owner_bug_nuim
      from (select cp.*, t3.title as spc_name, t6.*
            from (select cpxid,
                         cpxmc,
                         cpid,
                         name cpname
                  from ex_ods_pass_ecology_uf_productline as t1
                           left join ex_ods_pass_ecology_uf_product as t2 on t1.cpxid = t2.linaname
                  where cpxid in ('03', '02', '04', '05')) cp
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
                     left join ods_zentao_zt_bug as t6 on t6.project = t5.id
            where t2.deleted = '0'
              and t3.deleted = '0'
              and t5.model = 'waterfall'
              and t6.deleted = '0') as t
      group by cpxid,
               cpxmc,
               cpid,
               cpname, spc_name

      union all

      select cpxid,
             cpxmc,
             cpid,
             cpname,
             spc_name,
             count(distinct case when status != 'closed' then id else null end)   as spc_unre_bug_num,
             count(distinct case when activatedcount > 0 then id else null end)   as spc_second_active_bug_nuim,
             count(distinct case when openedby is not null then id else null end) as spc_owner_bug_nuim
      from (select cp.*, t3.title as spc_name, t6.*
            from (select cpxid,
                         cpxmc,
                         cpid,
                         name cpname
                  from ex_ods_pass_ecology_uf_productline as t1
                           left join ex_ods_pass_ecology_uf_product as t2 on t1.cpxid = t2.linaname
                  where cpxid in ('03', '02', '04', '05')) cp
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
                     left join ods_zentao_zt_bug as t6 on t6.execution = t5.id
            where t2.deleted = '0'
              and t3.deleted = '0'
              and t5.type = 'sprint'
              and t6.deleted = '0') as t
      group by cpxid,
               cpxmc,
               cpid,
               cpname, spc_name) as tt
group by cpxid,
         cpxmc,
         cpid,
         cpname,
         spc_name

-- 版本的转测不通过数：
-- 1.根据产品基础信息表.产品编码= zt_module.productId 找到产品的zt_module.id
-- 2.根据：zt_module.id = zt.product.line AND zt.product.deleted=0 找到对应产品的库的id（可能会有多个）
-- 3.根据：zt_productplan.product =  zt.product.id AND zt_productplan.deleted=0 找到产品所有的版本zt_productplan.id
-- 卡住 4.产品版本zt_productplan.id=zt_projectproduct.plan,被归属哪个项目或执行，判断版本测试单的统计方式：瀑布和敏捷的区分
--   若：zt_projectproduct.project =zt_project.id AND zt_project.module="waterfall"，找到能匹配上的zt_project.id
--   则：zt_testtask.project=zt_project.id AND zt_testtask.deleted=0 AND zt_testtask.status=“blocked” 找到产品版本下转测不通过的测试单数

-- 	若：zt_projectproduct.project =zt_project.id AND zt_project.type="sprint"，找到匹配版本的zt_project.id
-- 	则：zt_testtask.execution=zt_project.id AND  zt_testtask.deleted=0 AND zt_testtask.status=“blocked” 找到产品版本下转测不通过的测试单数
select cpxid,
       cpxmc,
       cpid,
       cpname,
       title,
       sum(spc_test_unnum) as spc_test_unnum, --版本的转测不通过数
       sum(spc_test_num)      spc_test_num    --版本提测轮次
from (select cpxid,
             cpxmc,
             cpid,
             cpname,
             title,
             count(distinct case when status = 'blocked' then id else null end) spc_test_unnum,--版本的转测不通过数
             count(distinct id)                                                 spc_test_num   --版本提测轮次
      from (select cp.*, t3.title, t6.*
            from (select cpxid,
                         cpxmc,
                         cpid,
                         name cpname
                  from ex_ods_pass_ecology_uf_productline as t1
                           left join ex_ods_pass_ecology_uf_product as t2 on t1.cpxid = t2.linaname
                  where cpxid in ('03', '02', '04', '05')) cp
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
                     left join ods_zentao_zt_testtask as t6 on t6.project = t5.id
            where t2.deleted = '0'
              and t3.deleted = '0'
              and t5.model = 'waterfall'
              and t6.deleted = '0') as t
      group by cpxid,
               cpxmc,
               cpid,
               cpname,
               title

      union all

      select cpxid,
             cpxmc,
             cpid,
             cpname,
             title,
             count(distinct case when status = 'blocked' then id else null end) spc_test_unnum,--版本的转测不通过数
             count(distinct id)                                                 spc_test_num   --版本提测轮次
      from (select cp.*, t3.title, t6.*
            from (select cpxid,
                         cpxmc,
                         cpid,
                         name cpname
                  from ex_ods_pass_ecology_uf_productline as t1
                           left join ex_ods_pass_ecology_uf_product as t2 on t1.cpxid = t2.linaname
                  where cpxid in ('03', '02', '04', '05')) cp
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
                     left join ods_zentao_zt_testtask as t6 on t6.execution = t5.id
            where t2.deleted = '0'
              and t3.deleted = '0'
              and t5.type = 'sprint'
              and t6.deleted = '0') as t
      group by cpxid,
               cpxmc,
               cpid,
               cpname,
               title) as tt
group by cpxid,
         cpxmc,
         cpid,
         cpname,
         title


-- 交付的任务数	1-3同上
-- 4.产品版本zt_productplan.id=zt_projectproduct.plan被归属哪个项目或执行，判断版本测试单的统计方式：瀑布和敏捷的区分
-- 若：zt_projectproduct.project =zt_project.id AND zt_project.module="waterfall"，找到能匹配上的zt_project.id
--则：zt_task.project=zt_project.id AND zt_task.deleted=0  AND zt_task.status=“closed”统计状态为已关闭的任务数

--若：zt_projectproduct.project =zt_project.id AND zt_project.type="sprint"，找到匹配版本的zt_project.id
--则：zt_task.execution=zt_project.id AND  zt_task.deleted=0  AND zt_task.status=“closed”统计状态为已关闭的任务数
select cpxid,
       cpxmc,
       cpid,
       cpname,
       title,
       sum(spc_done_num) as    spc_test_unnum,--交付的任务数
       sum(spc_owner_done_num) spc_owner_done_num --个人交付的任务数
from (select cpxid,
             cpxmc,
             cpid,
             cpname,
             title,
             count(distinct case when status = 'closed' then id else null end)                   spc_done_num,--交付的任务数
             count(distinct
                   case when status = 'closed' and finishedby is not null then id else null end) spc_owner_done_num--个人交付的任务数

      from (select cp.*, t3.title, t6.*
            from (select cpxid,
                         cpxmc,
                         cpid,
                         name cpname
                  from ex_ods_pass_ecology_uf_productline as t1
                           left join ex_ods_pass_ecology_uf_product as t2 on t1.cpxid = t2.linaname
                  where cpxid in ('03', '02', '04', '05')) cp
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
                     left join ods_zentao_zt_task as t6 on t6.project = t5.id
            where t2.deleted = '0'
              and t3.deleted = '0'
              and t5.model = 'waterfall'
              and t6.deleted = '0') as t
      group by cpxid,
               cpxmc,
               cpid,
               cpname,
               title

      union all

      select cpxid,
             cpxmc,
             cpid,
             cpname,
             title,
             count(distinct case when status = 'closed' then id else null end)                   spc_test_unnum,--版本的转测不通过数
             count(distinct
                   case when status = 'closed' and finishedby is not null then id else null end) spc_owner_done_num--个人交付的任务数
      from (select cp.*, t3.title, t6.*
            from (select cpxid,
                         cpxmc,
                         cpid,
                         name cpname
                  from ex_ods_pass_ecology_uf_productline as t1
                           left join ex_ods_pass_ecology_uf_product as t2 on t1.cpxid = t2.linaname
                  where cpxid in ('03', '02', '04', '05')) cp
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
                     left join ods_zentao_zt_task as t6 on t6.execution = t5.id
            where t2.deleted = '0'
              and t3.deleted = '0'
              and t5.type = 'sprint'
              and t6.deleted = '0') as t
      group by cpxid,
               cpxmc,
               cpid,
               cpname,
               title) as tt
group by cpxid,
         cpxmc,
         cpid,
         cpname,
         title



-- 计划交付需求数	版本的SR需求数：
-- 1.根据产品基础信息表.产品编码= zt_module.productId 找到产品的zt_module.id
-- 2.根据：zt_module.id = zt.product.line AND zt.product.deleted=0 找到对应产品的库的id（可能会有多个）
-- 3.根据：zt_productplan.product =  zt.product.id AND zt_productplan.deleted=0 找到产品所有的版本zt_productplan.id
-- 4.根据zt_productplan.id=zt_projectproduct.plan找到归属项目或执行的 zt_productlan.project字段，判断SR需求的统计方式：瀑布和敏捷的区分
--   若: zt_projectproduct.project =zt_project.id AND zt_project.module="waterfall"，找到能匹配上的zt_project.id
--    根据：zt_projectstory.project=zt_project.id 中所有的zt_projectstory.story  找到版本所有的需求但未区分是否删除
--    则： zt_projectstory.story=zt_story.id  AND zt_story.deleted=0  统计版本下未删除的SR需求数
--
--   若：zt_projectproduct.project =zt_project.id AND zt_project.type="sprint"，找到匹配版本的zt_project.id
-- 根据：zt_projectstory.project=zt_project.id 中所有的zt_projectstory.story  找到版本所有的需求但未区分是否删除
--    则： zt_projectstory.story=zt_story.id  AND zt_story.deleted=0  统计版本下未删除的SR需求数
select cpxid,
       cpxmc,
       cpid,
       cpname,
       spc_name,
       sum(spc_plan_demand_num) as spc_plan_demand_num,--计划交付需求数
       sum(spc_act_demand_num)  as spc_act_demand_num  --实际交付需求数
from (select cpxid,
             cpxmc,
             cpid,
             cpname,
             spc_name,
             count(distinct id)                                                spc_plan_demand_num,--计划交付需求数
             count(distinct case when status = 'closed' then id else null end) spc_act_demand_num--实际交付需求数
      from (select cp.*, t3.title spc_name, t7.*
            from (select cpxid,
                         cpxmc,
                         cpid,
                         name cpname
                  from ex_ods_pass_ecology_uf_productline as t1
                           left join ex_ods_pass_ecology_uf_product as t2 on t1.cpxid = t2.linaname
                  where cpxid in ('03', '02', '04', '05')) cp
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
                     left join ods_zentao_zt_projectstory as t6 on t6.project = t5.id
                     left join ods_zentao_zt_story as t7 on t7.id = t6.story
            where t2.deleted = '0'
              and t3.deleted = '0'
              and t5.model = 'waterfall'
              and t7.deleted = '0') as t
      group by cpxid,
               cpxmc,
               cpid,
               cpname,
               spc_name

      union all

      select cpxid,
             cpxmc,
             cpid,
             cpname,
             spc_name,
             count(distinct id)                                                spc_plan_demand_num,--计划交付需求数
             count(distinct case when status = 'closed' then id else null end) spc_act_demand_num--实际交付需求数
      from (select cp.*, t3.title spc_name, t7.*
            from (select cpxid,
                         cpxmc,
                         cpid,
                         name cpname
                  from ex_ods_pass_ecology_uf_productline as t1
                           left join ex_ods_pass_ecology_uf_product as t2 on t1.cpxid = t2.linaname
                  where cpxid in ('03', '02', '04', '05')) cp
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
                     left join ods_zentao_zt_projectstory as t6 on t6.project = t5.id
                     left join ods_zentao_zt_story as t7 on t7.id = t6.story
            where t2.deleted = '0'
              and t3.deleted = '0'
              and t5.type = 'sprint'
              and t7.deleted = '0') as t
      group by cpxid,
               cpxmc,
               cpid,
               cpname,
               spc_name) as tt
group by cpxid,
         cpxmc,
         cpid,
         cpname,
         spc_name


-- 测试用例数	1-3同上
-- 4.产品版本zt_productplan.id=zt_projectproduct.plan被归属哪个项目或执行，判断版本测试单的统计方式：瀑布和敏捷的区分
--     若：zt_projectproduct.project =zt_project.id AND zt_project.module="waterfall"，找到能匹配上的zt_project.id
--     则：zt_case.project=zt_project.id AND zt_case.deleted=0 AND zt_case.story不为空 and zt_case.story=zt.story.id统计版本下关联项目的测试用例数
--
--   若：zt_projectproduct.project =zt_project.id AND zt_project.type="sprint"，找到匹配版本的zt_project.id
--   则：zt_case.execution=zt_project.id AND  zt_case.deleted=0 AND zt_case.story不为空 and zt_case.story=zt.story.id统计版本下关联冲刺的测试用例数

select cpxid,
       cpxmc,
       cpid,
       cpname,
       spc_name,
       sum(spc_test_num)       as spc_test_num,--测试用例数
       sum(spc_owner_test_num) as spc_owner_test_num --个人执行的用例数
from (select cpxid,
             cpxmc,
             cpid,
             cpname,
             spc_name,
             count(distinct id)                                                     spc_test_num,--测试用例数
             count(distinct case when lastrunner is not null then id else null end) spc_owner_test_num --个人执行的用例数
      from (select cp.*, t3.title spc_name, t6.lastrunner, t7.*
            from (select cpxid,
                         cpxmc,
                         cpid,
                         name cpname
                  from ex_ods_pass_ecology_uf_productline as t1
                           left join ex_ods_pass_ecology_uf_product as t2 on t1.cpxid = t2.linaname
                  where cpxid in ('03', '02', '04', '05')) cp
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
                     left join ods_zentao_zt_case as t6 on t6.project = t5.id
                     left join ods_zentao_zt_story as t7 on t7.id = t6.story
            where t2.deleted = '0'
              and t3.deleted = '0'
              and t5.model = 'waterfall'
              and t6.deleted = '0'
              and t6.story is not null) as t
      group by cpxid,
               cpxmc,
               cpid,
               cpname,
               spc_name

      union all

      select cpxid,
             cpxmc,
             cpid,
             cpname,
             spc_name,
             count(distinct id)                                                     spc_test_num,--测试用例数
             count(distinct case when lastrunner is not null then id else null end) spc_owner_test_num--个人执行的用例数
      from (select cp.*, t3.title spc_name, t6.lastrunner, t7.*
            from (select cpxid,
                         cpxmc,
                         cpid,
                         name cpname
                  from ex_ods_pass_ecology_uf_productline as t1
                           left join ex_ods_pass_ecology_uf_product as t2 on t1.cpxid = t2.linaname
                  where cpxid in ('03', '02', '04', '05')) cp
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
                     left join ods_zentao_zt_case as t6 on t6.execution = t5.id
                     left join ods_zentao_zt_story as t7 on t7.id = t6.story
            where t2.deleted = '0'
              and t3.deleted = '0'
              and t5.type = 'sprint'
              and t6.deleted = '0'
              and t6.story is not null) as t
      group by cpxid,
               cpxmc,
               cpid,
               cpname,
               spc_name) as tt
group by cpxid,
         cpxmc,
         cpid,
         cpname,
         spc_name


-- 需求的任务数	版本下的需求任务数：
-- 1.根据：产品基础信息表.产品编码= zt_module.productId 找到产品的zt_module.id
-- 2.根据：zt_module.id = zt.product.line AND zt.product.deleted=0 找到对应产品的库的id（可能会有多个）
-- 3.根据：zt_productplan.product =  zt.product.id AND zt_productplan.deleted=0 找到产品所有的版本zt_productplan.id
-- 4.根据 ：zt_productplan.id=zt_projectproduct.plan找一个版本下所有的zt_projectproduct.project
-- 5.根据：zt_projectproduct.project =zt_project.id AND zt_project.module="waterfall" 匹配上的zt_project.id 区分瀑布项目
-- 5-1根据：zt_project.id=zt_projectstory.project，找到zt_projectstory.story作为版本的所有需求（包含未删除）
-- 5-2根据：zt_projectstory.story=zt_story.id，AND zt_story.deleted=0 找到版本所有未删除的需求
-- 5-3根据：zt_task.story=zt_story.id  AND zt_task .deleted=0  所有的zt_task.story的统计作为需求的任务数据

-- 6.根据：zt_projectproduct.project =zt_project.id AND zt_project.type="sprint"匹配上的zt_project.id区分scrum项目
-- 6-1根据：zt_project.id=zt_projectstory.project，找到zt_projectstory.story作为版本的所有需求（包含未删除）
-- 6-2根据：zt_projectstory.story=zt_story.id，AND zt_story.deleted=0 找到版本所有未删除的需求
-- 6-3根据：zt_task.story=zt_story.id  AND zt_task .deleted=0  所有的zt_task.story的统计作为需求的任务数据

select cpxid,
       cpxmc,
       cpid,
       cpname,
       spc_name,
       sum(spc_demand_task_num) as spc_demand_task_num--计划交付需求数
from (select cpxid,
             cpxmc,
             cpid,
             cpname,
             spc_name,
             count(distinct story) spc_demand_task_num--需求的任务数
      from (select cp.*, t3.title as spc_name, t8.*
            from (select cpxid,
                         cpxmc,
                         cpid,
                         name cpname
                  from ex_ods_pass_ecology_uf_productline as t1
                           left join ex_ods_pass_ecology_uf_product as t2 on t1.cpxid = t2.linaname
                  where cpxid in ('03', '02', '04', '05')) cp
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
                     left join ods_zentao_zt_projectstory as t6 on t6.project = t5.id
                     left join ods_zentao_zt_story as t7 on t7.id = t6.story
                     left join ods_zentao_zt_task as t8 on t8.story = t7.id
            where t2.deleted = '0'
              and t3.deleted = '0'
              and t5.model = 'waterfall'
              and t7.deleted = '0'
              and t8.deleted = '0') as t
      group by cpxid,
               cpxmc,
               cpid,
               cpname,
               spc_name

      union all

      select cpxid,
             cpxmc,
             cpid,
             cpname,
             spc_name,
             count(distinct story) spc_demand_task_num--需求的任务数
      from (select cp.*, t3.title as spc_name, t8.*
            from (select cpxid,
                         cpxmc,
                         cpid,
                         name cpname
                  from ex_ods_pass_ecology_uf_productline as t1
                           left join ex_ods_pass_ecology_uf_product as t2 on t1.cpxid = t2.linaname
                  where cpxid in ('03', '02', '04', '05')) cp
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
                     left join ods_zentao_zt_projectstory as t6 on t6.project = t5.id
                     left join ods_zentao_zt_story as t7 on t7.id = t6.story
                     left join ods_zentao_zt_task as t8 on t8.story = t7.id
            where t2.deleted = '0'
              and t3.deleted = '0'
              and t5.type = 'sprint'
              and t7.deleted = '0'
              and t8.deleted = '0') as t
      group by cpxid,
               cpxmc,
               cpid,
               cpname,
               spc_name) as tt
group by cpxid,
         cpxmc,
         cpid,
         cpname,
         spc_name


-- 需求的开发周期	对应表及字段：
-- 找developing操作时间
-- 1.根据：zt_action.objectID=zt_story.id AND zt_action.objecttype=story AND zt_action.action=edited找操作记录
-- 2.根据：zt_action.id=zt_history.action AND zt_history.filed='stage" 找到 设置阶段的记录
-- 3.根据：zt.history.action=zt_action.id AND  zt.history.new=“developing” 设置为开发中的所的 zt_action.id
-- 4.根据：zt_action.id 找到zt_action.date,取zt_action.date 最小的zt_action.id 所对应的zt_action.date（取日期）
-- 找developed操作时间
-- 1.根据：zt_action.objectID=zt_story.id AND zt_action.objecttype=story AND zt_action.action=edited找操作记录
-- 2.根据：zt_action.id=zt_history.action AND zt_history.filed='stage" 找到 设置阶段的记录
-- 3.根据：zt.history.action=zt_action.id AND  zt.history.new=“developed” 设置为开发中的所的 zt_action.id
-- 4.根据：zt_action.id 找到zt_action.date,取zt_action.date 最大的zt_action.id 所对应的zt_action.date（取日期）
-- 公式计算：
-- developed操作时间-developing操作时间+1
select deved.cpxid,
       deved.cpxmc,
       deved.cpid,
       deved.cpname,
       deved.spc_name,
       deved.action,
       developed_date,
       developing_date,
       (developed_date - developing_date) + 1 as develop_days --需求的开发周期
from (select cpxid,
             cpxmc,
             cpid,
             cpname,
             spc_name,
             action,
             max(event_date) as developed_date
      from (select cp.*, t3.title as spc_name, date(t8.date) as event_date, t9.action
            from (select cpxid,
                         cpxmc,
                         cpid,
                         name cpname
                  from ex_ods_pass_ecology_uf_productline as t1
                           left join ex_ods_pass_ecology_uf_product as t2 on t1.cpxid = t2.linaname
                  where cpxid in ('03', '02', '04', '05')) cp
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
                     left join ods_zentao_zt_projectstory as t6 on t6.project = t5.id
                     left join ods_zentao_zt_story as t7 on t7.id = t6.story
                     left join ods_zentao_zt_action as t8 on t8.objectid = t7.id
                     join ods_zentao_zt_history as t9 on t9.action = t8.id
            where t2.deleted = '0'
              and t3.deleted = '0'
              and t5.model = 'waterfall'
              and t7.deleted = '0'
              and t8.objecttype = 'story'
              and t8.action = 'edited'
              and t9.field = 'stage'
              and t9.new = 'developed') as t
      group by cpxid,
               cpxmc,
               cpid,
               cpname,
               spc_name, action) deved
         left join
     (select cpxid,
             cpxmc,
             cpid,
             cpname,
             spc_name,
             action,
             min(event_date) as developing_date
      from (select cp.*, t3.title as spc_name, date(t8.date) as event_date, t9.action
            from (select cpxid,
                         cpxmc,
                         cpid,
                         name cpname
                  from ex_ods_pass_ecology_uf_productline as t1
                           left join ex_ods_pass_ecology_uf_product as t2 on t1.cpxid = t2.linaname
                  where cpxid in ('03', '02', '04', '05')) cp
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
                     left join ods_zentao_zt_projectstory as t6 on t6.project = t5.id
                     left join ods_zentao_zt_story as t7 on t7.id = t6.story
                     left join ods_zentao_zt_action as t8 on t8.objectid = t7.id
                     join ods_zentao_zt_history as t9 on t9.action = t8.id
            where t2.deleted = '0'
              and t3.deleted = '0'
              and t5.model = 'waterfall'
              and t7.deleted = '0'
              and t8.objecttype = 'story'
              and t8.action = 'edited'
              and t9.field = 'stage'
              and t9.new = 'developing') as t
      group by cpxid,
               cpxmc,
               cpid,
               cpname,
               spc_name, action) deving
     on deved.cpxid = deving.cpxid and
        deved.cpxmc = deving.cpxmc and
        deved.cpid = deving.cpid and
        deved.cpname = deving.cpname and
        deved.spc_name = deving.spc_name and
        deved.action = deving.action


--
-- 需求的验证周期
-- 找developed操作时间
-- 1.根据：zt_action.objectID=zt_story.id AND zt_action.objecttype=story AND zt_action.action=edited找操作记录
-- 2.根据：zt_action.id=zt_history.action AND zt_history.filed='stage" 找到 设置阶段的记录
-- 3.根据：zt.history.action=zt_action.id AND  zt.history.new=“developed” 设置为开发完成的所的 zt_action.id
-- 4.根据：zt_action.id 找到zt_action.date,取zt_action.date 最小的zt_action.id 所对应的zt_action.date（取日期）
--
-- 找tested操作时间
-- 1.根据：zt_action.objectID=zt_story.id AND zt_action.objecttype=story AND zt_action.action=edited找操作记录
-- 2.根据：zt_action.id=zt_history.action AND zt_history.filed='stage" 找到 设置阶段的记录
-- 3.根据：zt.history.action=zt_action.id AND  zt.history.new=“tested” 设置为测试完毕的所的 zt_action.id
-- 4.根据：zt_action.id 找到zt_action.date,取zt_action.date 最大的zt_action.id 所对应的zt_action.date（取日期）
--
-- 公式计算：
-- tested操作时间-developed操作时间+1

select deved.cpxid,
       deved.cpxmc,
       deved.cpid,
       deved.cpname,
       deved.spc_name,
       deved.action,
       developed_date,
       tested_date,
       (tested_date-developed_date) + 1 as develop_days --需求的开发周期
from (select cpxid,
             cpxmc,
             cpid,
             cpname,
             spc_name,
             action,
             min(event_date) as developed_date
      from (select cp.*, t3.title as spc_name, date(t8.date) as event_date, t9.action
            from (select cpxid,
                         cpxmc,
                         cpid,
                         name cpname
                  from ex_ods_pass_ecology_uf_productline as t1
                           left join ex_ods_pass_ecology_uf_product as t2 on t1.cpxid = t2.linaname
                  where cpxid in ('03', '02', '04', '05')) cp
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
                     left join ods_zentao_zt_projectstory as t6 on t6.project = t5.id
                     left join ods_zentao_zt_story as t7 on t7.id = t6.story
                     left join ods_zentao_zt_action as t8 on t8.objectid = t7.id
                     join ods_zentao_zt_history as t9 on t9.action = t8.id
            where t2.deleted = '0'
              and t3.deleted = '0'
              and t5.model = 'waterfall'
              and t7.deleted = '0'
              and t8.objecttype = 'story'
              and t8.action = 'edited'
              and t9.field = 'stage'
              and t9.new = 'developed') as t
      group by cpxid,
               cpxmc,
               cpid,
               cpname,
               spc_name, action) deved
         left join
     (select cpxid,
             cpxmc,
             cpid,
             cpname,
             spc_name,
             action,
             max(event_date) as tested_date
      from (select cp.*, t3.title as spc_name, date(t8.date) as event_date, t9.action
            from (select cpxid,
                         cpxmc,
                         cpid,
                         name cpname
                  from ex_ods_pass_ecology_uf_productline as t1
                           left join ex_ods_pass_ecology_uf_product as t2 on t1.cpxid = t2.linaname
                  where cpxid in ('03', '02', '04', '05')) cp
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
                     left join ods_zentao_zt_projectstory as t6 on t6.project = t5.id
                     left join ods_zentao_zt_story as t7 on t7.id = t6.story
                     left join ods_zentao_zt_action as t8 on t8.objectid = t7.id
                     join ods_zentao_zt_history as t9 on t9.action = t8.id
            where t2.deleted = '0'
              and t3.deleted = '0'
              and t5.model = 'waterfall'
              and t7.deleted = '0'
              and t8.objecttype = 'story'
              and t8.action = 'edited'
              and t9.field = 'stage'
              and t9.new = 'tested') as t
      group by cpxid,
               cpxmc,
               cpid,
               cpname,
               spc_name, action) deving
     on deved.cpxid = deving.cpxid and
        deved.cpxmc = deving.cpxmc and
        deved.cpid = deving.cpid and
        deved.cpname = deving.cpname and
        deved.spc_name = deving.spc_name and
        deved.action = deving.action



-- 提交的BUG数	版本的bug
-- 1.根据产品基础信息表.产品编码= zt_module.productId 找到产品的zt_module.id
-- 2.根据：zt_module.id = zt.product.line AND zt.product.deleted=0 找到对应产品的库的id（可能会有多个）
-- 3.根据：zt_productplan.product =  zt.product.id AND zt_productplan.deleted=0 找到产品所有的版本zt_productplan.id
-- 4.产品版本zt_productplan.id=zt_projectproduct.plan ,被归属哪个项目或执行，判断版本bug的统计方式：瀑布和敏捷bug的区分
--   若：zt_projectproduct.project =zt_project.id AND zt_project.module="waterfall"，找到能匹配上的zt_project.id
--   则：zt_bug.project=zt_project.id AND zt_bug.deleted=0  找打产品版本的bug
--         再根据zt_bug.openedBy不为空，找到个人提交的bug数
--
--   若：zt_projectproduct.project =zt_project.id AND zt_project.type="sprint"，找到匹配版本的zt_project.id
--   则：zt_bug.execution=zt_project.id AND  zt_bug.deleted=0 找打产品版本的bug
--   再根据zt_bug.openedBy不为空，找到个人提交的bug数
--答案见最初bug的需求




-- 需求的BUG数	版本的SR需求数：
-- 1.根据产品基础信息表.产品编码= zt_module.productId 找到产品的zt_module.id
-- 2.根据：zt_module.id = zt.product.line AND zt.product.deleted=0 找到对应产品的库的id（可能会有多个）
-- 3.根据：zt_productplan.product =  zt.product.id AND zt_productplan.deleted=0 找到产品所有的版本zt_productplan.id
-- 4.根据zt_productplan.id=zt_projectproduct.plan找到归属项目或执行的 zt_productlan.project字段，判断SR需求的统计方式：瀑布和敏捷的区分
--   若: zt_projectproduct.project =zt_project.id AND zt_project.module="waterfall"，找到能匹配上的zt_project.id
--    根据：zt_projectstory.project=zt_project.id 中所有的zt_projectstory.story  找到版本所有的需求但未区分是否删除
--    则： zt_projectstory.story=zt_story.id  AND zt_story.deleted=0  统计版本下未删除的SR需求数
--
--   若：zt_projectproduct.project =zt_project.id AND zt_project.type="sprint"，找到匹配版本的zt_project.id
-- 根据：zt_projectstory.project=zt_project.id 中所有的zt_projectstory.story  找到版本所有的需求但未区分是否删除
--    则： zt_projectstory.story=zt_story.id  AND zt_story.deleted=0  统计版本下未删除的SR需求数
-- 5.根据zt_story.id=zt_bug.story，找到每条需求关联的bug数
select cpxid,
       cpxmc,
       cpid,
       cpname,
       spc_name,
       sum(spc_demand_bug_num) as spc_demand_bug_num--需求的BUG数
from (select cpxid,
             cpxmc,
             cpid,
             cpname,
             spc_name,
             count(distinct story) spc_demand_bug_num--计划交付需求数
      from (select cp.*, t3.title spc_name, t8.*
            from (select cpxid,
                         cpxmc,
                         cpid,
                         name cpname
                  from ex_ods_pass_ecology_uf_productline as t1
                           left join ex_ods_pass_ecology_uf_product as t2 on t1.cpxid = t2.linaname
                  where cpxid in ('03', '02', '04', '05')) cp
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
                     left join ods_zentao_zt_projectstory as t6 on t6.project = t5.id
                     left join ods_zentao_zt_story as t7 on t7.id = t6.story
                     left join ods_zentao_zt_bug as t8 on t8.story = t7.id
            where t2.deleted = '0'
              and t3.deleted = '0'
              and t5.model = 'waterfall'
              and t7.deleted = '0') as t
      group by cpxid,
               cpxmc,
               cpid,
               cpname,
               spc_name

      union all

      select cpxid,
             cpxmc,
             cpid,
             cpname,
             spc_name,
             count(distinct story) spc_demand_bug_num--计划交付需求数
      from (select cp.*, t3.title spc_name, t8.*
            from (select cpxid,
                         cpxmc,
                         cpid,
                         name cpname
                  from ex_ods_pass_ecology_uf_productline as t1
                           left join ex_ods_pass_ecology_uf_product as t2 on t1.cpxid = t2.linaname
                  where cpxid in ('03', '02', '04', '05')) cp
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
                     left join ods_zentao_zt_projectstory as t6 on t6.project = t5.id
                     left join ods_zentao_zt_story as t7 on t7.id = t6.story
                     left join ods_zentao_zt_bug as t8 on t8.story = t7.id
            where t2.deleted = '0'
              and t3.deleted = '0'
              and t5.type = 'sprint'
              and t7.deleted = '0') as t
      group by cpxid,
               cpxmc,
               cpid,
               cpname,
               spc_name) as tt
group by cpxid,
         cpxmc,
         cpid,
         cpname,
         spc_name



-- 需求的用例数	版本的SR需求数：
-- 1.根据产品基础信息表.产品编码= zt_module.productId 找到产品的zt_module.id
-- 2.根据：zt_module.id = zt.product.line AND zt.product.deleted=0 找到对应产品的库的id（可能会有多个）
-- 3.根据：zt_productplan.product =  zt.product.id AND zt_productplan.deleted=0 找到产品所有的版本zt_productplan.id
-- 4.根据zt_productplan.id=zt_projectproduct.plan找到归属项目或执行的 zt_productlan.project字段，判断SR需求的统计方式：瀑布和敏捷的区分
--   若: zt_projectproduct.project =zt_project.id AND zt_project.module="waterfall"，找到能匹配上的zt_project.id
--    根据：zt_projectstory.project=zt_project.id 中所有的zt_projectstory.story  找到版本所有的需求但未区分是否删除
--    则： zt_projectstory.story=zt_story.id  AND zt_story.deleted=0  统计版本下未删除的SR需求数
--
--   若：zt_projectproduct.project =zt_project.id AND zt_project.type="sprint"，找到匹配版本的zt_project.id
-- 根据：zt_projectstory.project=zt_project.id 中所有的zt_projectstory.story  找到版本所有的需求但未区分是否删除
--    则： zt_projectstory.story=zt_story.id  AND zt_story.deleted=0  统计版本下未删除的SR需求数
-- 5.根据zt_story.id=zt_case.story，找到每条需求关联的用例数
select cpxid,
       cpxmc,
       cpid,
       cpname,
       spc_name,
       sum(spc_demand_case_num) as spc_demand_case_num--需求的用例数
from (select cpxid,
             cpxmc,
             cpid,
             cpname,
             spc_name,
             count(distinct story) spc_demand_case_num--需求的用例数
      from (select cp.*, t3.title spc_name, t8.*
            from (select cpxid,
                         cpxmc,
                         cpid,
                         name cpname
                  from ex_ods_pass_ecology_uf_productline as t1
                           left join ex_ods_pass_ecology_uf_product as t2 on t1.cpxid = t2.linaname
                  where cpxid in ('03', '02', '04', '05')) cp
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
                     left join ods_zentao_zt_projectstory as t6 on t6.project = t5.id
                     left join ods_zentao_zt_story as t7 on t7.id = t6.story
                     left join ods_zentao_zt_case as t8 on t8.story = t7.id
            where t2.deleted = '0'
              and t3.deleted = '0'
              and t5.model = 'waterfall'
              and t7.deleted = '0') as t
      group by cpxid,
               cpxmc,
               cpid,
               cpname,
               spc_name

      union all

      select cpxid,
             cpxmc,
             cpid,
             cpname,
             spc_name,
             count(distinct story) spc_demand_case_num--需求的用例数
      from (select cp.*, t3.title spc_name, t8.*
            from (select cpxid,
                         cpxmc,
                         cpid,
                         name cpname
                  from ex_ods_pass_ecology_uf_productline as t1
                           left join ex_ods_pass_ecology_uf_product as t2 on t1.cpxid = t2.linaname
                  where cpxid in ('03', '02', '04', '05')) cp
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
                     left join ods_zentao_zt_projectstory as t6 on t6.project = t5.id
                     left join ods_zentao_zt_story as t7 on t7.id = t6.story
                     left join ods_zentao_zt_case as t8 on t8.story = t7.id
            where t2.deleted = '0'
              and t3.deleted = '0'
              and t5.type = 'sprint'
              and t7.deleted = '0') as t
      group by cpxid,
               cpxmc,
               cpid,
               cpname,
               spc_name) as tt
group by cpxid,
         cpxmc,
         cpid,
         cpname,
         spc_name