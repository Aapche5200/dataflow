-- 工单详情表-核心
truncate table ads_itr_workorder_detail;
insert into ads_itr_workorder_detail
select a.id               ,-- as 工单ID,
       Date(a.gmt_create) ,--  as 建单时间,
       a.order_code       ,--  as 工单编号,
       a.order_title      ,--  as 工单标题,
       a.product_id       ,--  as 产品ID,
       a.product_type_name ,-- as 产品类型,
       a.product_version   ,-- as 产品版本,
       b.show_name    problem_big    ,-- as 问题大类,
       c.show_name    problem_small     ,-- as 问题小类,
       d.show_name     wordorder_level    ,-- as 工单等级,
       f.hetong_id         ,-- as 合同编号,
       f.kehu_name         ,-- as 最终客户名称,
       f.field0380 ,-- as 第一行业,
       f.field0381 ,-- as 第二行业,
       f.shangji_name      ,-- as 项目名称,
       case
           when a.work_order_status = 0 then '草稿'
           when a.work_order_status = 1 then '未受理'
           when a.work_order_status = 6 then '待派发'
           when a.work_order_status = 2 then '受理中'
           when a.work_order_status = 3 then '已处理'
           when a.work_order_status = 5 then '已关闭'
           else null end  work_order_status ,-- as 工单状态,
       a.problem_desc      ,-- as 问题描述,
       g.field_info       ,--  as 自定义字段,
       h.id      as bug_id          ,-- as BUG记录ID,
       i.l1                ,-- as L1处理人,
       i.l2               ,--  as L2处理人,
       i.l3                ,-- as L3处理人,
       j.gmt_modified     ,--  as 关单时间,
       j.close_time       ,--  as 关单时长,
       j.hang_time    hand_up_time    ,--  as 挂起时长,
       k.solution         ,--  as 解决方案,
       k.cause_analysis    ,-- as 原因分析,
       j.hang_time        response_time -- as 响应时长  --todo 临时字段，后续调整这个字段的查询逻辑
from ex_ods_itr_workorder_work_order a
         FULL OUTER JOIN ex_ods_itr_workorder_dictionary b on a.problem_big_type = b.id
         FULL OUTER JOIN ex_ods_itr_workorder_dictionary c on a.problem_small_type = c.id
         FULL OUTER JOIN ex_ods_itr_workorder_dictionary d on a.order_level = d.id
         FULL OUTER JOIN (select "ID", formmain_id
                          from ex_ods_oa_abv5_formson_1939
                          union all
                          select "ID", formmain_id
                          from ex_ods_oa_abv5_formson_18956
                          union all
                          select "ID", formmain_id
                          from ex_ods_oa_abv5_formson_19389) e on a.product_line_id = cast(e."ID" as varchar(64))
         FULL OUTER JOIN (select "ID", field0002 as hetong_id, field0004 as kehu_name, field0976 as shangji_name,field0380 as field0380,field0381 as field0381
                          from ex_ods_oa_abv5_formmain_1119
                          union all
                          select a."ID", field0003 as hetong_id, field0005 as kehu_name, field0015 as shangji_name,b."SHOWVALUE" as field0380,c."SHOWVALUE" as field0381
                          from ex_ods_oa_abv5_formmain_18955 a
                              inner join ex_ods_oa_abv5_ctp_enum_item b on a.field0171=cast(b."ID" as varchar(64))
                              inner join ex_ods_oa_abv5_ctp_enum_item c on a.field0172=cast(c."ID" as varchar(64)) ) f on e.formmain_id = f."ID"

         FULL OUTER JOIN ex_ods_itr_workorder_work_order_info g on g.order_id = a.id --自定义字段查询
         FULL OUTER JOIN ex_ods_itr_workorder_bug_record h on a.id = h.order_id -- BUG记录查询
         inner join (select a.id, c.handler_name as l1, d.handler_name as l2, e.handler_name as l3
                     from ex_ods_itr_workorder_work_order a
                              inner join ex_ods_itr_workorder_flow_table b on a.id = b.biz_id
                              left join (select *
                                         from (select a.id,
                                                      b.node_name,
                                                      b.handler_code,
                                                      b.handler_name,
                                                      ROW_NUMBER()
                                                      OVER (PARTITION BY flow_id, node_name ORDER BY b.gmt_create DESC) AS rn
                                               from ex_ods_itr_workorder_flow_table a
                                                        inner join ex_ods_itr_workorder_flow_node b on a.id = b.flow_id
                                               where b.node_name in ('L1提单', 'L1处理')) a
                                         where a.rn = 1) c on c.id = b.id
                              left join (select *
                                         from (select a.id,
                                                      b.node_name,
                                                      b.handler_code,
                                                      b.handler_name,
                                                      ROW_NUMBER()
                                                      OVER (PARTITION BY flow_id, node_name ORDER BY b.gmt_create DESC) AS rn
                                               from ex_ods_itr_workorder_flow_table a
                                                        inner join ex_ods_itr_workorder_flow_node b on a.id = b.flow_id
                                               where b.node_name in ('L2处理')) a
                                         where a.rn = 1) d on d.id = b.id
                              left join (select *
                                         from (
                                                  select a.id,
                                                         b.node_name,
                                                         b.handler_code,
                                                         b.handler_name,
                                                         ROW_NUMBER()
                                                         OVER (PARTITION BY flow_id, node_name ORDER BY b.gmt_create DESC) AS rn
                                                  from ex_ods_itr_workorder_flow_table a
                                                           inner join ex_ods_itr_workorder_flow_node b on a.id = b.flow_id
                                                  where b.node_name in ('L3处理')) a
                                         where a.rn = 1) e on e.id = b.id) i on i.id = a.id -- L1/L2/L3处理人查询
         FULL OUTER JOIN (select a.id,
                                 d.gmt_modified,
                                 a.gmt_create,
                                 b.hang_time,
                                 EXTRACT(EPOCH FROM (d.gmt_modified - a.gmt_create)) - b.hang_time AS close_time
                          from ex_ods_itr_workorder_work_order a
                                   inner join (
                              select order_id,
                                     sum(EXTRACT(EPOCH FROM (gmt_modified - hang_start_time))) AS hang_time
                              from ex_ods_itr_workorder_hang_up
                              group by order_id
                          ) b on b.order_id = a.id
                                   inner join ex_ods_itr_workorder_flow_table c on c.biz_id = a.id
                                   inner join ex_ods_itr_workorder_flow_node d on d.flow_id = c.id
                          where a.work_order_status = 5
                            and d.node_name = '已关闭'
                          union all
                          select a.id,
                                 d.gmt_modified,
                                 a.gmt_create,
                                 null                                              as hang_time,
                                 EXTRACT(EPOCH FROM (d.gmt_create - a.gmt_create)) AS close_time
                          from ex_ods_itr_workorder_work_order a
                                   inner join (select a.id
                                               from ex_ods_itr_workorder_work_order a
                                                        left join ex_ods_itr_workorder_hang_up b on a.id = b.order_id
                                               where b.id is null
                                                 and a.work_order_status = 5) b on a.id = b.id
                                   inner join ex_ods_itr_workorder_flow_table c on c.biz_id = a.id
                                   inner join ex_ods_itr_workorder_flow_node d on d.flow_id = c.id
                          where d.node_name = '已关闭') j on j.id = a.id -- 关单时长查询
         FULL OUTER JOIN (select a.id, b.solution, b.cause_analysis
                          from ex_ods_itr_workorder_work_order a
                                   inner join ex_ods_itr_workorder_solution_record b on a.id = b.order_id
                                   inner join (
                              select id
                              from (select id,
                                           ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY gmt_create DESC) AS rn
                                    from ex_ods_itr_workorder_solution_record) a
                              where a.rn = 1
                          ) c on c.id = b.id) k on k.id = a.id --解决方案和原因分析

where a.work_order_status <> 0;

--创建索引
CREATE INDEX index_id ON ads_itr_workorder_detail (id);
CREATE INDEX index_gmt_modified ON ads_itr_workorder_detail (gmt_modified);

-- 工单操作操作记录表
truncate table ads_itr_operation_detail;
insert into ads_itr_operation_detail
select a.id            ,  -- as 操作记录ID,
       a.order_id      ,   -- as 工单ID,
       a.operator     ,    -- as 处理人工号,
       b.node_name    ,    -- as 流转节点,
       a.operator_name ,   -- as 处理人,
       a.operation_record , --as 操作记录,
       a.operation_time ,   --as 处理时间,
       case
           when a.operation_type = 0 then '提单'
           when a.operation_type = 1 then '编辑'
           when a.operation_type = 2 then '接单'
           when a.operation_type = 3 then '提交解决方案'
           when a.operation_type = 4 then '升级'
           when a.operation_type = 5 then '驳回'
           when a.operation_type = 6 then '确认关单'
           when a.operation_type = 7 then '挂起'
           when a.operation_type = 8 then '手动解除挂起'
           when a.operation_type = 9 then '转交'
           when a.operation_type = 10 then '发起群聊'
           when a.operation_type = 11 then '加入群聊'
           when a.operation_type = 12 then '发起回溯'
           when a.operation_type = 13 then '回溯分析'
           when a.operation_type = 14 then '措施审核'
           when a.operation_type = 15 then '问题修复'
           when a.operation_type = 16 then '回溯关闭'
           when a.operation_type = 17 then '关联工单'
           when a.operation_type = 18 then '派发'
           when a.operation_type = 19 then '修改问题信息'
           when a.operation_type = 20 then '修改解决方案'
           when a.operation_type = 21 then '自动接单'
           when a.operation_type = 22 then '自动解除挂起'
           when a.operation_type = 23 then '接收派单'
           when a.operation_type = 24 then '撤回'
           when a.operation_type = 25 then '重新派发'
           when a.operation_type = 26 then '取消派发'
           when a.operation_type = 27 then 'BUG已解决'
           when a.operation_type = 28 then '需求已解决'
           when a.operation_type = 29 then '发送客户短信'
           else null end  operation_type --as 操作类型

from ex_ods_itr_workorder_operation_record a
         inner join ex_ods_itr_workorder_flow_node b on a.node_id = b.id;

--创建索引
CREATE INDEX index_order_id ON ads_itr_operation_detail (order_id);

-- 产品自定义工单内容表
truncate table ads_itr_product_custominfo;
insert into  ads_itr_product_custominfo
select product_id ,--as 产品ID,
       field_name ,--as 自定义字段,
       id         -- 自定义记录ID
from ex_ods_itr_auth_custom_configuration
where deleted = 0;


-- 人员一次解决-人员信息
truncate table ads_itr_workorder_personnel;
insert into  ads_itr_workorder_personnel
select a.biz_id      , --as 工单编号,
       c.handler_name, --as 处理人姓名,
       c.handler_code ,--as 处理人工号,
       c.duty_code,   --as 责任人工号,
       c.duty_name   --as 责任人姓名
from ex_ods_itr_workorder_flow_table a
         inner join (select *
                     from (
                              select flow_id, node_name, count(node_name) as number
                              from ex_ods_itr_workorder_flow_node
                              where node_name = 'L3处理'
                              group by flow_id, node_name) a
                     where a.number = 1) b on a.id = b.flow_id
         inner join ex_ods_itr_workorder_flow_node c on c.flow_id = b.flow_id
         inner join ex_ods_itr_workorder_work_order d on d.id = a.biz_id
where c.node_name = 'L3处理';



-- 逾期时间分布
truncate table ads_itr_overdue_scatter;
insert into ads_itr_overdue_scatter
-- 整单逾期 /响应逾期，
select order_id,
       overdue_start_time,
       ABS(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Shanghai' AT TIME ZONE 'Asia/Shanghai' - overdue_start_time)) AS overdue_time,0 as type
from ex_ods_itr_workorder_overdue_record a
         inner join ex_ods_itr_workorder_work_order b on a.order_id=b.id
where overdue_state = 0
  and overdue_type = 3
-- 整单逾期已结束 * 60   --新增调整
union all
select order_id,
       overdue_start_time,
       overdue_time * 60,
       0 as type
from ex_ods_itr_workorder_overdue_record a
         inner join ex_ods_itr_workorder_work_order b on a.order_id = b.id
where overdue_state = 1
  and overdue_type = 3

union all
-- 响应逾期,已结束
select
       order_id, overdue_start_time,overdue_time*60,1 as type

from ex_ods_itr_workorder_overdue_record a
inner join ex_ods_itr_workorder_work_order b on a.order_id=b.id
where overdue_type = 1
  and overdue_state = 1

-- 响应逾期,未结束
union all
select
       order_id, overdue_start_time,  ABS(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Shanghai' - overdue_start_time)) AS overdue_time,1 as type

from ex_ods_itr_workorder_overdue_record a
         inner join ex_ods_itr_workorder_work_order b on a.order_id=b.id
where overdue_type = 1
  and overdue_state = 0
;