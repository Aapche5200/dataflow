select * from ods_task_job_execute_log
         --where job_level='dws'
         order by end_time desc

select * from ods_task_job_execute_log
         where date(end_time)=date(current_date-interval '1 days')

select * from ods_task_job_execute_log
         where job_level='ads'
         order by end_time

update ods_task_job_execute_log
set job_owner='尹书山'

select * from  ods_task_job_schedule_pool;


update ods_task_job_schedule_pool
 set   job_sql='insert into ads_gp_oa_t_area_unit_sales_dd
select *
from (select t1.date_y,
             t1.date_q,
             t1.date_m,
             t1.confirm_date,
             t1.area,
             t1.area_unit,
             IIF(t1.income - t2.current_income = null, 0, t1.income - t2.current_income)         as income,
             IIF(t1.unincome - t2.current_unincome = null, 0, t1.unincome - t2.current_unincome) as unincome
      from (select year_date                  date_y,
                   季度                       date_q,
                   date_m,
                   确认收入日期               confirm_date,
                   大区                       area,
                   考核单位                   area_unit,
                   round(sum(确认收入), 0) as income,
                   round(sum(待确认收入), 0)  unincome
            from (SELECT field0004                                                                           料号,
                         大区名称                                                                            大区,
                         case
                             when field0012 = ''北京渠道事业部'' then ''渠道事业部(北京分公司)''
                             when field0012 = ''军民融合事业部二部'' then ''军民融合事业二部''
                             else field0012 end                                                              考核单位,
                         field0067                                                                           产品名称,
                         field0108                                                                           合同年份,
                         field0049                                                                           实施情况,
                         substring(cast(convert(varchar(100), field0297, 120) as varchar(100)), 1, 4)        year_date,
                         concat(''Q'', datepart(quarter, field0297))                                           季度,
                         concat(datepart(month, field0297), ''月'')                                            date_m,
                         convert(varchar(100), field0297, 23)                                                确认收入日期,
                         field0291                                                                           待确认收入类型,
                         field0002                                                                           合同,
                         sum(field0278)                                                                      确认收入,
                         SUM(case when field0291 is not null then field0290 else 0 end)                      待确认收入,
                         sum(case when field0066 = ''产品'' then field0103 else 0 end)                         出货量,
                         sum(field0046)                                                                      产品实际毛利,
                         sum(case when (field0029 <> ''否'' or field0078 <> ''其他'') then field0016 else 0 end) 合同产品金额
                  FROM formmain_105321 AS t1
                           join
                       (select t1.大区名称, t1."三级部门ID", "NAME"
                        from saleDepartLevel as t1
                                 join org_unit as t2 on t1."三级部门ID" = t2."ID"
                        where t1.大区名称 in (''北分管理部'', ''北区管理部'', ''南区管理部'', ''战略合作部'', ''长沙办'', ''上海大区'')
                        group by t1.大区名称, t1."三级部门ID", "NAME") as t2
                       on case
                              when field0012 = ''北京渠道事业部'' then ''渠道事业部(北京分公司)''
                              when field0012 = ''军民融合事业部二部'' then ''军民融合事业二部''
                              else field0012 end = t2."NAME"
                           join (select field0002 as contract_code
                                 from formmain_1119
                                 where coalesce(field1039, ''其他'') <> ''已注销'') as t3 on t1.field0002 = t3.contract_code
                  where coalesce(t1.field0027, ''其他'') <> ''内部结算''
                  GROUP BY field0004, 大区名称,
                           case
                               when field0012 = ''北京渠道事业部'' then ''渠道事业部(北京分公司)''
                               when field0012 = ''军民融合事业部二部'' then ''军民融合事业二部''
                               else field0012 end, field0067,
                           field0108,
                           field0049,
                           substring(cast(convert(varchar(100), field0297, 120) as varchar(100)), 1, 4),
                           concat(''Q'', datepart(quarter, field0297)),
                           concat(datepart(month, field0297), ''月''),
                           convert(varchar(100), field0297, 23),
                           field0291, field0002) as t1
            where 大区 is not null
            group by year_date,
                     季度,
                     date_m,
                     确认收入日期,
                     大区,
                     考核单位) as t1
               left join (select cast(area as varchar(800))                as area,
                                 cast(area_unit as varchar(800))           as area_unit,
                                 cast(date_y as varchar(800))              as date_y,
                                 max(confirm_date)                         as confirm_date,
                                 round(sum(coalesce(income_diff, 0)), 0)   as current_income,
                                 round(sum(coalesce(unincome_diff, 0)), 0) as current_unincome
                          from ads_gp_oa_t_area_unit_sales_dd
                          group by cast(area as varchar(800))
                                 , cast(area_unit as varchar(800))
                                 , cast(date_y as varchar(800))) as t2
                         on t1.date_y = t2.date_y and t1.area = t1.area and t1.area_unit = t2.area_unit and
                            t1.confirm_date <> t2.confirm_date) as m
where income <> 0
   or unincome <> 0;'
where job_name ='func_ads_gp_oa_t_area_unit_sales_dd'

insert into temp_ods_gp_job_schedule_pool
(job_db,
job_name,
job_level,
level_sort,
job_sql,
job_desc,
job_owner)
values ('oa',
        'func_ads_gp_oa_t_area_unit_sales_dd',
       'ads',
       111,
       'insert into ads_gp_oa_t_area_unit_sales_dd
select *
from(
select t1.date_y,
       t1.date_q,
       t1.date_m,
       t1.confirm_date,
       t1.area,
       t1.area_unit,
       IIF(t1.income - t2.current_income = null,0,t1.income - t2.current_income)      as income,
       IIF(t1.unincome - t2.current_unincome = null,0,t1.unincome - t2.current_unincome) as unincome
from (select year_date                  date_y,
             季度                       date_q,
             date_m,
             确认收入日期               confirm_date,
             大区                       area,
             考核单位                   area_unit,
             round(sum(确认收入), 0) as income,
             round(sum(待确认收入), 0)  unincome
      from (SELECT field0004                                                                           料号,
                   大区名称                                                                            大区,
                   case
                       when field0012 = ''北京渠道事业部'' then ''渠道事业部(北京分公司)''
                       when field0012 = ''军民融合事业部二部'' then ''军民融合事业二部''
                       else field0012 end                                                              考核单位,
                   field0067                                                                           产品名称,
                   field0108                                                                           合同年份,
                   field0049                                                                           实施情况,
                   substring(cast(convert(varchar(100), field0297, 120) as varchar(100)), 1, 4)        year_date,
                   concat(''Q'', datepart(quarter, field0297))                                           季度,
                   concat(datepart(month, field0297), ''月'')                                            date_m,
                   convert(varchar(100), field0297, 23)                                                确认收入日期,
                   field0291                                                                           待确认收入类型,
                   field0002                                                                           合同,
                   sum(field0278)                                                                      确认收入,
                   SUM(case when field0291 is not null then field0290 else 0 end)                      待确认收入,
                   sum(case when field0066 = ''产品'' then field0103 else 0 end)                         出货量,
                   sum(field0046)                                                                      产品实际毛利,
                   sum(case when (field0029 <> ''否'' or field0078 <> ''其他'') then field0016 else 0 end) 合同产品金额
            FROM formmain_105321 AS t1
                     join
                 (select t1.大区名称, t1."三级部门ID", "NAME"
                  from saleDepartLevel as t1
                           join org_unit as t2 on t1."三级部门ID" = t2."ID"
                  where t1.大区名称 in (''北分管理部'', ''北区管理部'', ''南区管理部'', ''战略合作部'', ''长沙办'', ''上海大区'')
                  group by t1.大区名称, t1."三级部门ID", "NAME") as t2
                 on case
                        when field0012 = ''北京渠道事业部'' then ''渠道事业部(北京分公司)''
                        when field0012 = ''军民融合事业部二部'' then ''军民融合事业二部''
                        else field0012 end = t2."NAME"
            GROUP BY field0004, 大区名称,
                     case
                         when field0012 = ''北京渠道事业部'' then ''渠道事业部(北京分公司)''
                         when field0012 = ''军民融合事业部二部'' then ''军民融合事业二部''
                         else field0012 end, field0067,
                     field0108,
                     field0049,
                     substring(cast(convert(varchar(100), field0297, 120) as varchar(100)), 1, 4),
                     concat(''Q'', datepart(quarter, field0297)),
                     concat(datepart(month, field0297), ''月''),
                     convert(varchar(100), field0297, 23),
                     field0291, field0002) as t1
      where 大区 is not null
      group by year_date,
               季度,
               date_m,
               确认收入日期,
               大区,
               考核单位) as t1
     left   join (select cast(area as varchar(800))                as area,
                      cast(area_unit as varchar(800))           as area_unit,
                      cast(date_y as varchar(800))              as date_y,
                      max(confirm_date)                         as confirm_date,
                      round(sum(coalesce(income_diff, 0)), 0)   as current_income,
                      round(sum(coalesce(unincome_diff, 0)), 0) as current_unincome
               from ads_gp_oa_t_area_unit_sales_dd
               group by cast(area as varchar(800))
                      , cast(area_unit as varchar(800))
                      , cast(date_y as varchar(800))) as t2
              on t1.date_y = t2.date_y and t1.area = t1.area and t1.area_unit = t2.area_unit and
                 t1.confirm_date <> t2.confirm_date
) as m
where income <>0 or unincome<>0;',
       '服务增量数据插入',
       '尹书山'),
 ('oa',
        'func_formmain_129544',
       'ads',
       201,
       'update formmain_129544
set field0018=gx.q2_done_lv,--q2完成率
    field0014=gx.q2_mubiao,--q2目标
    field0008=gx.q1_mubiao,--q1目标
    field0001=gx.confirm_date,--确认收入日期
    field0006=gx.current_income,--当前年累计确认收入
    field0007=gx.current_unincome,--当前年累计待确认收入
    field0005=gx.year_mubiao,--年目标
    field0035=gx.four_m_income,--4月实际收入
    field0016=gx.q2_income,--q2实际收入
    field0036=gx.five_m_income,--5月实际收入
    field0049=gx.seven_m_income,--7月实际收入
    field0050=gx.eight_m_income,--8月实际收入
    field0063=gx.ten_m_income,--10月实际收入
    field0064=gx.eleven_m_income,--11月实际收入
    field0015=gx.q2_predict_income,--q2预测收入
    field0021=gx.q3_predict_income,--q3预测收入
    field0027=gx.q4_predict_income,--q4预测收入
    field0020=gx.q3_mubiao, --	Q3收入目标
    field0026=gx.q4_mubiao, --	Q4收入目标
    field0017=gx.q2_predict_done_lv, --	Q2预测完成率
    field0023=gx.q3_predict_done_lv, --	Q3预测完成率
    field0024=gx.q3_done_lv, --	Q3收入完成率
    field0029=gx.q4_predict_done_lv, --		Q4预测完成率
    field0030=gx.q4_done_lv, --	Q4收入完成率
    field0089=gx.six_m_income, --	六月实际收入
    field0090=gx.nine_m_income, --	九月实际收入
    field0091=gx.twelve_m_income --	十二月实际收入
from (select event_date                                                                                    as confirm_date,
             t1.area,
             t1.area_unit,
             t1.date_y,
             round(t1.current_income / 10000, 2)                                                              current_income,
             round(t4.unincome / 10000, 2)                                                                    current_unincome,
             round(t1.q1_income / 10000, 2)                                                                   q1_income,
             round(t2.q1_mubiao / 10000, 2)                                                                   q1_mubiao,
             round(q1_income / t2.q1_mubiao, 4)                                                               q1_done_lv,
             round(t2.year_mubiao / 10000, 2)                                                                 year_mubiao,
             round((t2.year_mubiao * 0.32 - t1.q1_income) / 10000, 2)                                      as q2_mubiao,
             round(t1.q2_income / 10000, 2)                                                                   q2_income,
             round(t1.q2_income / (t2.year_mubiao * 0.32 - t1.q1_income), 4)                                  q2_done_lv,
             round(t1.q3_income / 10000, 2)                                                                   q3_income,
             iif(q3_income = 0, 0, round((t2.year_mubiao * 0.6 - q1_income - q2_income) / 10000, 2))       as q3_mubiao,
             round(q3_income / (t2.year_mubiao * 0.6 - q1_income - q2_income), 4)                          as q3_done_lv,
             round(t1.q4_income / 10000, 2)                                                                   q4_income,
             iif(q4_income = 0, 0, round((t2.year_mubiao - q1_income - q2_income - q3_income) / 10000, 2)) as q4_mubiao,
             round(q4_income / (t2.year_mubiao - q1_income - q2_income - q3_income),
                   4)                                                                                      as q4_done_lv,
             round(t1.four_m_income / 10000, 2)                                                               four_m_income,
             round(five_m_income / 10000, 2)                                                                  five_m_income,
             round(six_m_income / 10000, 2)                                                                   six_m_income,
             round(seven_m_income / 10000, 2)                                                                 seven_m_income,
             round(eight_m_income / 10000, 2)                                                                 eight_m_income,
             round(nine_m_income / 10000, 2)                                                                  nine_m_income,
             round(ten_m_income / 10000, 2)                                                                   ten_m_income,
             round(eleven_m_income / 10000, 2)                                                                eleven_m_income,
             round(twelve_m_income / 10000, 2)                                                                twelve_m_income,
             t3.q2_predict_income,
             round((t3.q2_predict_income * 10000) / ((t2.year_mubiao * 0.32 - t1.q1_income)),
                   4)                                                                                         q2_predict_done_lv,
             t3.q3_predict_income,
             round((t3.q3_predict_income * 10000) / (t2.year_mubiao * 0.6 - q1_income - q2_income),
                   4)                                                                                         q3_predict_done_lv,
             t3.q4_predict_income,
             round((t3.q4_predict_income * 10000) / (t2.year_mubiao - q1_income - q2_income - q3_income),
                   4)                                                                                         q4_predict_done_lv
      from (--增量计算
               select cast(area as varchar(800))                                                       as area,
                      cast(area_unit as varchar(800))                                                  as area_unit,
                      cast(date_y as varchar(800))                                                     as date_y,
                      max(confirm_date)                                                                as confirm_date,
                      sum(coalesce(income_diff, 0))                                                    as current_income,
                      sum(coalesce(unincome_diff, 0))                                                  as current_unincome,
                      sum(case when cast(date_q as varchar(800)) = ''Q1'' then income_diff else 0 end)   as q1_income,
                      sum(case when cast(date_q as varchar(800)) = ''Q2'' then income_diff else 0 end)   as q2_income,
                      sum(case when cast(date_q as varchar(800)) = ''Q3'' then income_diff else 0 end)   as q3_income,
                      sum(case when cast(date_q as varchar(800)) = ''Q4'' then income_diff else 0 end)   as q4_income,
                      sum(case when cast(date_m as varchar(800)) = ''4月'' then income_diff else 0 end)  as four_m_income,
                      sum(case when cast(date_m as varchar(800)) = ''5月'' then income_diff else 0 end)  as five_m_income,
                      sum(case when cast(date_m as varchar(800)) = ''6月'' then income_diff else 0 end)  as six_m_income,
                      sum(case
                              when cast(date_m as varchar(800)) = ''7月'' then income_diff
                              else 0 end)                                                              as seven_m_income,
                      sum(case
                              when cast(date_m as varchar(800)) = ''8月'' then income_diff
                              else 0 end)                                                              as eight_m_income,
                      sum(case when cast(date_m as varchar(800)) = ''9月'' then income_diff else 0 end)  as nine_m_income,
                      sum(case when cast(date_m as varchar(800)) = ''10月'' then income_diff else 0 end) as ten_m_income,
                      sum(case
                              when cast(date_m as varchar(800)) = ''11月'' then income_diff
                              else 0 end)                                                              as eleven_m_income,
                      sum(case
                              when cast(date_m as varchar(800)) = ''12月'' then income_diff
                              else 0 end)                                                              as twelve_m_income
               from ads_gp_oa_t_area_unit_sales_dd
               group by cast(area as varchar(800))
                      , cast(area_unit as varchar(800))
                      , cast(date_y as varchar(800))) as t1
               left join( --目标
          select 大区名称 as                  大区,
                 "NAME"   as                  办事处,
                 全年收入目标 * 10000         year_mubiao,
                 全年收入目标 * 10000 * 0.128 q1_mubiao
          from (select 大区名称, "三级部门ID", sum(field0044) 全年收入目标
                from formmain_5137 as t1
                         left join saleDepartLevel as t2 on t1.field0002 = t2."销售部门ID"
                where 大区名称 in (''北区管理部'', ''南区管理部'')
                  and t1.field0004 = 2023
                  and t1.field0007 = ''8559535201370522547''
                  and t1.field0044 is not null
                group by 大区名称, "三级部门ID") as t1
                   join org_unit as t2 on t1."三级部门ID" = t2."ID"

          union all

          SELECT 大区名称,
                 case when "NAME" = ''合肥办'' then ''安徽办'' else "NAME" END name,
                 全年收入目标 * 10000                                      year_mubiao,
                 全年收入目标 * 10000 * 0.128                              q1_mubiao
          FROM (select field0002, "NAME", sum(field0044) 全年收入目标
                from formmain_5137 as t1
                         join org_unit as t2 on t1.field0002 = t2."ID"
                WHERE field0004 = 2023
                  and field0007 = ''8559535201370522547''
                  and field0044 is not null
                group by field0002, "NAME") AS t1
                   join saleDepartLevel as t2 on t1.field0002 = t2."销售部门ID"
          where 大区名称 in (''北分管理部'', ''长沙办'', ''上海大区'', ''战略合作部'')) as t2
                        on t1.area = t2.大区 and t1.area_unit = t2.办事处
               left join ( --预测
          select field0002,
                 field0003,
                 field0004,
                 sum(coalesce(field0042, 0)) +
                 sum(coalesce(field0043, 0)) +
                 sum(coalesce(field0044, 0)) +
                 sum(coalesce(field0045, 0)) +
                 sum(coalesce(field0046, 0)) +
                 sum(coalesce(field0047, 0)) +
                 sum(coalesce(field0048, 0)) as q2_predict_income,
                 sum(coalesce(field0056, 0)) +
                 sum(coalesce(field0057, 0)) +
                 sum(coalesce(field0058, 0)) +
                 sum(coalesce(field0059, 0)) +
                 sum(coalesce(field0060, 0)) +
                 sum(coalesce(field0061, 0)) +
                 sum(coalesce(field0062, 0)) as q3_predict_income,
                 sum(coalesce(field0070, 0)) +
                 sum(coalesce(field0071, 0)) +
                 sum(coalesce(field0072, 0)) +
                 sum(coalesce(field0073, 0)) +
                 sum(coalesce(field0074, 0)) +
                 sum(coalesce(field0075, 0)) +
                 sum(coalesce(field0076, 0)) as q4_predict_income
          from formmain_129544
          group by field0002, field0003, field0004) as t3
                         on t1.area = t3.field0002 and t1.area_unit = t3.field0003 and t1.date_y = t3.field0004
               left join (select field0001                      area,
                                 field0011                      area_unit,
                                 sum(coalesce(field0045, 0)) +
                                 sum(coalesce(field0043, 0)) +
                                 sum(coalesce(field0044, 0)) +
                                 sum(coalesce(field0046, 0)) as unincome
                          from formmain_128838 as t1
                          where field0104 is not null
                            and field0001 is not null
                          group by field0001,
                                   field0011) as t4 on t1.area = t4.area and t1.area_unit = t4.area_unit
               left join (select max(convert(varchar(100), field0297, 23)) as event_date from formmain_105321) as t5
                         on 1 = 1) as gx
where field0002 = gx.area
  and field0003 = gx.area_unit;',
       '服务表单数据更新',
       '尹书山')