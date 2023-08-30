--itr 研发/质量基础data 产品维度
--ads_gp_pg_t_itr_basic_cp_df
create table ads_gp_pg_t_itr_basic_cp_df as
(select a.product_type_name               as 产品类型,
        Date(a.gmt_create)                as 日期,
        'ITR工单客户数量'                 as 指标,
        count(distinct a.product_line_id) as 指标值
 from ds_itr_workorder_work_order a
 where a.work_order_status <> 0
   and a.gmt_create >= date_trunc('month', current_date - interval '3 month') -- 1 week,1 month,3 month
   and a.gmt_create < date_trunc('month', current_date)
 group by a.product_type_name,
          Date(a.gmt_create)

 union all

 select a.product_type_name  as 产品类型,
        Date(a.gmt_create)   as 日期,
        'ITR工单数量'        as 指标,
        count(distinct a.id) as 指标值
 from ds_itr_workorder_work_order a
 where a.work_order_status <> 0
   and a.gmt_create >= date_trunc('month', current_date - interval '3 month') -- 1 week,1 month,3 month
   and a.gmt_create < date_trunc('month', current_date)
 group by a.product_type_name,
          Date(a.gmt_create)

 union all

 select a.product_type_name    as 产品类型,
        Date(a.gmt_create)     as 日期,
        'L3软件问题的工单数量' as 指标,
        count(distinct a.id)   as 指标值
 from ds_itr_workorder_work_order a
          inner join ds_itr_workorder_flow_table b on a.id = b.biz_id
          inner join ds_itr_workorder_flow_node c on b.id = c.flow_id
 where c.node_name = 'L3处理'
   and a.work_order_status <> 0
   and a.problem_big_type = 1554645069768249346
   and a.gmt_create >= date_trunc('month', current_date - interval '3 month') -- 1 week,1 month,3 month
   and a.gmt_create < date_trunc('month', current_date)
 group by a.product_type_name,
          Date(a.gmt_create)

 union all

 select a.product_type_name as 产品类型,
        Date(a.gmt_create)  as 日期,
        'L3升单闭环率'      as 指标,
        ROUND((count(case when c.node_name = 'L3处理' then 1 else null end) / count(distinct a.id) ::NUMERIC),
              2)            as 指标值
 from ds_itr_workorder_work_order a
          inner join ds_itr_workorder_flow_table b on a.id = b.biz_id
          inner join ds_itr_workorder_flow_node c on b.id = c.flow_id
 where a.work_order_status <> 0
   and a.gmt_create >= date_trunc('month', current_date - interval '3 month') -- 1 week,1 month,3 month
   and a.gmt_create < date_trunc('month', current_date)
 group by a.product_type_name,
          Date(a.gmt_create)

 union all

 select a.product_type_name      as 产品类型,
        Date(a.gmt_create)       as 日期,
        '工单数量'               as 指标,
        count(product_type_name) as 指标值
 from ds_itr_workorder_work_order a
 where a.work_order_status <> 0
   and gmt_create >= date_trunc('month', current_date - interval '3 month') -- 1 week,1 month,3 month
   and gmt_create < date_trunc('month', current_date)
 group by Date(a.gmt_create),
          a.product_type_name)





--itr 研发/质量基础data 新增遗留分布
--ads_gp_pg_t_itr_newold_fenbu_df
create table ads_gp_pg_t_itr_newold_fenbu_df as
select a.product_type_name                                                     as 产品类型,
       Date(a.gmt_create)                                                      as 日期,
       a.product_version                                                       as 版本名称,
       count(a.product_line_id)                                                as 反馈项目总数,
       count(case when a.work_order_status not in (0, 5) then 1 else null end) as 产品内部未解决数,
       count(case when a.work_order_status not in (0, 5) then 1 else null end) as 遗留未解决项目数一暂定,
       ROUND((count(case when a.work_order_status not in (0, 5) then 1 else null end) / count(distinct a.id) ::NUMERIC),
             2)                                                                as 闭环率
from ds_itr_workorder_work_order a
where a.work_order_status <> 0
  and a.gmt_create >= date_trunc('month', current_date - interval '3 month') -- 1 week,1 month,3 month
  and a.gmt_create < date_trunc('month', current_date)
group by a.product_type_name,
         a.product_version,
         Date(a.gmt_create)


--itr 研发/质量基础data 用户问题分布
create table ads_gp_pg_t_itr_lastuser_fenbu_df as
select a.product_type_name                 as 产品类型,
       Date(a.gmt_create)                  as 日期,
       a.product_version                   as 版本名称,
       a.ultimate_customer                 as 最终客户,
       count(distinct a.ultimate_customer) as 工单数量
from ds_itr_workorder_work_order a
where a.work_order_status <> 0
--    and a.ultimate_customer
group by a.ultimate_customer,
         a.product_type_name,
         Date(a.gmt_create),
         a.product_version


--问题类型分布
create table ads_gp_pg_t_itr_qtype_fenbu_df as
select a.product_type_name as 产品类型,
       Date(a.gmt_create)  as 日期,
       a.product_version   as 版本名称,
       b.show_name         as 问题类型,
       count(b.show_name)  as 产品数量
from ds_itr_workorder_work_order a
         inner join ds_itr_workorder_dictionary b on a.problem_small_type = b.id
where a.work_order_status <> 0
  and a.gmt_create >= date_trunc('month', current_date - interval '3 month') -- 1 week,1 month,3 month
  and a.gmt_create < date_trunc('month', current_date)
group by a.product_type_name,
         b.show_name,
         Date(a.gmt_create),
         a.product_version


--工单等级分布
create table ads_gp_pg_t_itr_gongdan_fenbu_df as
select a.product_type_name as 产品类型,
       Date(a.gmt_create)  as 日期,
       a.product_version   as 版本信息,
       b.show_name         as 工单等级,
       count(b.show_name)  as 工单数量
from ds_itr_workorder_work_order a
         inner join ds_itr_workorder_dictionary b on a.order_level = b.id
where a.work_order_status <> 0 --    and
      --    a.gmt_create >= '2022-12-02 00:00:00'
      --    and a.gmt_create <= '2022-12-08 23:00:59'
group by a.product_type_name,
         b.show_name,
         Date(a.gmt_create),
         a.product_version


--产品问题趋势
create table ads_gp_pg_t_itr_cp_qushi_df as
select a.product_type_name  as 产品类型,
       Date(a.gmt_create)   as 日期,
       count(distinct a.id) as 产品新增问题数,
       count(
               case
                   when a.work_order_status not in (0, 5) then 1
                   else null
                   end
           )                as 产品内部未解决数,
       ROUND(
                   count(
                           case
                               when a.work_order_status = 5 then 1
                               else null
                               end
                       ) / count(DISTINCT a.id):: NUMERIC,
                   2
           )                as 问题闭环率
from ds_itr_workorder_work_order a
where a.work_order_status <> 0
  and a.gmt_create >= date_trunc('month', current_date - interval '3 month') -- 1 week,1 month,3 month
  and a.gmt_create < date_trunc('month', current_date)
group by Date(a.gmt_create),
         a.product_type_name



--客户行业分布
drop table ads_gp_pg_t_itr_user_hangye_df
create table ads_gp_pg_t_itr_user_hangye_df as
with formson_table as (select id, formmain_id
                       from ds_oa_formson_1939
                       union all
                       select id, formmain_id
                       from ds_oa_formson_18956
                       union all
                       select id, formmain_id
                       from ds_oa_formson_19389)
select a.product_type_name               as cptype,--产品类型,
       a.product_version as banben_name ,--版本名称
       Date(a.gmt_create)               as event_date,--日期,
       --a.product_line_id                 as 合同号一需要匹配行业,
       c.field0380                       as first_cate,--第一行业,
       c.field0381                       as second_cate,--第二行业,
       count(distinct a.product_line_id) as gongdan_qty--工单数量
from ds_itr_workorder_work_order a
         inner join formson_table b on product_line_id = cast(b.id as varchar(64))
         inner join ds_oa_formmain_1119 c on b.formmain_id = c.id
where a.work_order_status <> 0
--   and a.gmt_create >= date_trunc('month', current_date - interval '1 month') -- 1 week,1 month,3 month
--   and a.gmt_create < date_trunc('month', current_date)
group by a.product_type_name,
         a.product_version,
         --a.product_line_id,
         Date(a.gmt_create),
         c.field0380,
         c.field0381




select date_trunc('week', current_date)
 date_trunc('week', current_date - interval '1 week')

--
select cptype,
       case
           when event_date < date_trunc('week', current_date) and
                event_date >= date_trunc('week', current_date - interval '1 week') then '第12周'

           when event_date < date_trunc('week', current_date - interval '1 week') and
                event_date >= date_trunc('week', current_date - interval '2 week') then '第11周'

           when event_date < date_trunc('week', current_date - interval '2 week') and
                event_date >= date_trunc('week', current_date - interval '3 week') then '第10周'

           when event_date < date_trunc('week', current_date - interval '3 week') and
                event_date >= date_trunc('week', current_date - interval '4 week') then '第9周'

           when event_date < date_trunc('week', current_date - interval '4 week') and
                event_date >= date_trunc('week', current_date - interval '5 week') then '第8周'

           when event_date < date_trunc('week', current_date - interval '5 week') and
                event_date >= date_trunc('week', current_date - interval '6 week') then '第7周'

           when event_date < date_trunc('week', current_date - interval '6 week') and
                event_date >= date_trunc('week', current_date - interval '7 week') then '第6周'

           when event_date < date_trunc('week', current_date - interval '7 week') and
                event_date >= date_trunc('week', current_date - interval '8 week') then '第5周'

           when event_date < date_trunc('week', current_date - interval '8 week') and
                event_date >= date_trunc('week', current_date - interval '9 week') then '第4周'

           when event_date < date_trunc('week', current_date - interval '9 week') and
                event_date >= date_trunc('week', current_date - interval '10 week') then '第3周'


           when event_date < date_trunc('week', current_date - interval '10 week') and
                event_date >= date_trunc('week', current_date - interval '11 week') then '第2周'


           when event_date < date_trunc('week', current_date - interval '11 week') and
                event_date >= date_trunc('week', current_date - interval '12 week') then '第1周'
           else null end                      tag,
       sum(cp_new_qty)                     as cp_new_qty,
       sum(cp_weijj_qty)                   as cp_weijj_qty,
       sum(cp_weijj_qty) / sum(cp_new_qty) as qa_lv
from ads_gp_pg_t_itr_cp_qushi_df
where event_date < date_trunc('week', current_date)
  and event_date >= date_trunc('week', current_date - interval '12 week')
group by cptype,
         case
             when event_date < date_trunc('week', current_date) and
                  event_date >= date_trunc('week', current_date - interval '4 week') then '近4周'
             when event_date < date_trunc('week', current_date) and
                  event_date >= date_trunc('week', current_date - interval '8 week') then '近8周'
             when event_date < date_trunc('week', current_date) and
                  event_date >= date_trunc('week', current_date - interval '12 week') then '近12周'
             else null end


case
             when event_date < date_trunc('week', current_date) and
                  event_date >= date_trunc('week', current_date - interval '1 week') then '近4周'
             when event_date < date_trunc('week', current_date) and
                  event_date >= date_trunc('week', current_date - interval '8 week') then '近8周'
             when event_date < date_trunc('week', current_date) and
                  event_date >= date_trunc('week', current_date - interval '12 week') then '近12周'
             else null end


    select  * from ds_ecology_uf_product  where cpid='124'  limit 10




ads_gp_pg_t_itr_basic_cp_df
ads_gp_pg_t_itr_newold_fenbu_df
ads_gp_pg_t_itr_lastuser_fenbu_df
ads_gp_pg_t_itr_qtype_fenbu_df
ads_gp_pg_t_itr_gongdan_fenbu_df
ads_gp_pg_t_itr_cp_qushi_df
ads_gp_pg_t_itr_user_hangye_df






