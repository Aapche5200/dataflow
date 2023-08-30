--产品维度bug数相关需求-按天维度
-- 产品的bug:
-- 1.根据产品基础信息表.产品编码= zt_module.productId 找到产品的zt_module.id
-- 2.根据：zt_module.id = zt.product.line AND zt.product.deleted=0 找到对应产品的bug库的id（可能会有多个）
-- 3.根据：zt_bug.product=zt.product.id AND zt_bug.deleted=0 的bug,就是产品的bug
select event_date,
       cpxid,
       cpxmc,
       cpid,
       cpname,
       count(distinct id)                                        as cp_bug_num--bug 总数
       --count(case when status = 'closed' then id else null end)  as unre_bug_nuim, --产品未解决的bug数
       --count(case when activatedcount > 0 then id else null end) as second_active_bug_nuim --产品二次激活bug数
from (select cpxid,
             cpxmc,
             cpid,
             cpname,
             module_id,
             pro_id,
             b.id,
             b.product,
             b.status,
             b.activatedcount,
             date(openeddate) as event_date
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
                     join ods_zentao_zt_product as p on cast(cpm.module_id as text) = cast(p.line as text)
            where p.deleted = '0') as cpmp
               join ods_zentao_zt_bug as b on cpmp.pro_id = b.product
      where b.deleted = '0'
        and date(openeddate) >= date('2023-01-01')) as t
group by event_date,
         cpxid,
         cpxmc,
         cpid,
         cpname

--产品维度bug数相关需求-实时
select cpxid,
       cpxmc,
       cpid,
       cpname,
       --count(distinct id)                                        as cp_bug_num,--bug 总数
       count(case when status = 'closed' then id else null end)  as unre_bug_num, --产品未解决的bug数
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
             b.activatedcount,
             date(openeddate) as event_date
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
                     join ods_zentao_zt_product as p on cast(cpm.module_id as text) = cast(p.line as text)
            where p.deleted = '0') as cpmp
               join ods_zentao_zt_bug as b on cpmp.pro_id = b.product
      where b.deleted = '0'
        and date(openeddate) >= date('2023-01-01')) as t
group by cpxid,
         cpxmc,
         cpid,
         cpname


--版本提测不通过次数 实时维度
-- 产品的转测不通过数：
-- 1、根据产品基础信息表.产品编码= zt_module.productId 找到产品的zt_module.id
-- 2.根据：zt_module.id = zt.product.line AND zt.product.deleted=0 找到对应产品的需求库的id（可能会有多个）
-- 3.根据：zt_testtask.product=zt.product.id AND zt_testtask.deleted=0
-- AND zt_testtask.status=“blocked”的测试单,就是转测不通过的测试单数
select cpxid,
       cpxmc,
       cpid,
       cpname,
       count(distinct case when status = 'blocked' then id else null end) as tice_butongguo_num, --版本提测不通过次数
       count(distinct id)                                                 as tice_num            --版本提测轮次
from (select cp.*, t3.*
      from (select cpxid,
                   cpxmc,
                   cpid,
                   name cpname
            from ex_ods_pass_ecology_uf_productline as t1
                     left join ex_ods_pass_ecology_uf_product as t2 on t1.cpxid = t2.linaname
            where cpxid in ('03', '02', '04', '05')) cp
               left join ods_zentao_zt_module as t1 on t1.produtctid = cp.cpid
               left join ods_zentao_zt_product as t2 on t2.line = t1.id
               left join ods_zentao_zt_testtask as t3 on t3.product = t2.id
      where t2.deleted = '0'
        and t3.deleted = '0') as t
group by cpxid,
         cpxmc,
         cpid,
         cpname



-- 交付的任务数
-- 1-2同上
-- 3.根据：zt_product.id=zt.projectproduct.product，找到产品下所有的项目
-- 4.根据：zt_project.id=zt_projectproduct.project，找到项目所属的产品
-- zt_task.project=zt.project.id AND zt_task.deleted=0 AND zt_task.status=“closed”统计状态为已关闭的任务数
select cpxid,
       cpxmc,
       cpid,
       cpname,
       count(distinct case when status = 'closed' then id else null end) as task_num--交付任务数
from (select cp.*, t5.*
      from (select cpxid,
                   cpxmc,
                   cpid,
                   name cpname
            from ex_ods_pass_ecology_uf_productline as t1
                     left join ex_ods_pass_ecology_uf_product as t2 on t1.cpxid = t2.linaname
            where cpxid in ('03', '02', '04', '05')) cp
               left join ods_zentao_zt_module as t1 on t1.produtctid = cp.cpid
               left join ods_zentao_zt_product as t2 on t2.line = t1.id
               left join ods_zentao_zt_projectproduct as t3 on t3.product = t2.id
               left join ods_zentao_zt_project as t4 on t4.id = t3.project
               left join ods_zentao_zt_task as t5 on t5.project = t4.id
      where t2.deleted = '0'
        and t5.deleted = '0') as t
group by cpxid,
         cpxmc,
         cpid,
         cpname

--计划交付需求数	1-2同上
-- 3.根据：zt_story.product=zt.product.id AND zt_story.deleted=0 AND
-- zt_story.type="story"统计产品SR需求数
select cpxid,
       cpxmc,
       cpid,
       cpname,
       count(distinct case when type = 'story' then id else null end)                       as sr_num,--计划交付需求数
       count(distinct case when type = 'story' and status = 'closed' then id else null end) as close_sr_num--实际交付需求数
from (select cp.*, t3.*
      from (select cpxid,
                   cpxmc,
                   cpid,
                   name cpname
            from ex_ods_pass_ecology_uf_productline as t1
                     left join ex_ods_pass_ecology_uf_product as t2 on t1.cpxid = t2.linaname
            where cpxid in ('03', '02', '04', '05')) cp
               left join ods_zentao_zt_module as t1 on t1.produtctid = cp.cpid
               left join ods_zentao_zt_product as t2 on t2.line = t1.id
               left join ods_zentao_zt_story as t3 on t3.product = t2.id
      where t2.deleted = '0'
        and t3.deleted = '0') as t
group by cpxid,
         cpxmc,
         cpid,
         cpname


--测试用例数	1-2同上
-- 3.根据：zt_product.id=zt.projectproduct.product，找到产品下所有的项目
-- 4.根据：zt_project.id=zt_projectproduct.project，找到项目所属的产品
-- zt_case.project=zt.project.id AND zt_case.deleted=0 AND zt_case.story不为空 and zt_case.story=zt.story.id统计测试用例数
select cpxid,
       cpxmc,
       cpid,
       cpname,
       count(distinct id) as test_num --测试用例数
from (select cp.*, t6.*
      from (select cpxid,
                   cpxmc,
                   cpid,
                   name cpname
            from ex_ods_pass_ecology_uf_productline as t1
                     left join ex_ods_pass_ecology_uf_product as t2 on t1.cpxid = t2.linaname
            where cpxid in ('03', '02', '04', '05')) cp
               left join ods_zentao_zt_module as t1 on t1.produtctid = cp.cpid
               left join ods_zentao_zt_product as t2 on t2.line = t1.id
               left join ods_zentao_zt_projectproduct as t3 on t3.product = t2.id
               left join ods_zentao_zt_project as t4 on t4.id = t3.project
               left join ods_zentao_zt_case as t5 on t5.project = t4.id
               left join ods_zentao_zt_story as t6 on t6.id = t5.story
      where t2.deleted = '0'
        and t5.deleted = '0'
        and t5.story is not null) as t
group by cpxid,
         cpxmc,
         cpid,
         cpname;

