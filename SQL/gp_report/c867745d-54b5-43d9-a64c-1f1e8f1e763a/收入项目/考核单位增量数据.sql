insert into ads_gp_oa_t_area_unit_sales_dd
select *
from(
select t1.date_y,
       t1.date_q,
       t1.date_m,
       t1.confirm_date,
       t1.area,
       t1.area_unit,
       nullif(t1.income - t2.current_income = null,0,t1.income - t2.current_income)      as income,
       nullif(t1.unincome - t2.current_unincome = null,0,t1.unincome - t2.current_unincome) as unincome
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
                       when field0012 = '北京渠道事业部' then '渠道事业部(北京分公司)'
                       when field0012 = '军民融合事业部二部' then '军民融合事业二部'
                       else field0012 end                                                              考核单位,
                   field0067                                                                           产品名称,
                   field0108                                                                           合同年份,
                   field0049                                                                           实施情况,
                   substr(to_char(field0297, 'YYYY-MM-DD'), 1, 4)                                      year_date,
                   concat('Q', extract(quarter from field0297))                                        季度,
                   concat(substr(to_char(field0297, 'YYYY-MM-DD'), 6, 2), '月')                        date_m,
                   date(field0297)                                                                     确认收入日期,
                   field0291                                                                           待确认收入类型,
                   field0002                                                                           合同,
                   sum(field0278)                                                                      确认收入,
                   SUM(case when field0291 is not null then field0290 else 0 end)                      待确认收入,
                   sum(case when field0066 = '产品' then field0103 else 0 end)                         出货量,
                   sum(field0046)                                                                      产品实际毛利,
                   sum(case when (field0029 <> '否' or field0078 <> '其他') then field0016 else 0 end) 合同产品金额
            FROM ex_ods_oa_abv5_formmain_105321 AS t1
                     join
                 (select t1.大区名称, t1."三级部门ID", "NAME"
                  from ex_ods_oa_abv5_saleDepartLevel as t1
                           join ex_ods_oa_abv5_org_unit as t2 on t1."三级部门ID" = cast(t2."ID" as text)
                  where t1.大区名称 in ('北分管理部', '北区管理部', '南区管理部', '战略合作部', '长沙办', '上海大区')
                  group by t1.大区名称, t1."三级部门ID", "NAME") as t2
                 on case
                        when field0012 = '北京渠道事业部' then '渠道事业部(北京分公司)'
                        when field0012 = '军民融合事业部二部' then '军民融合事业二部'
                        else field0012 end = t2."NAME"
            GROUP BY field0004, 大区名称,
                     case
                         when field0012 = '北京渠道事业部' then '渠道事业部(北京分公司)'
                         when field0012 = '军民融合事业部二部' then '军民融合事业二部'
                         else field0012 end, field0067,
                     field0108,
                     field0049,
                     substr(to_char(field0297, 'YYYY-MM-DD'), 1, 4),
                     concat('Q', extract(quarter from field0297)),
                     concat(substr(to_char(field0297, 'YYYY-MM-DD'), 6, 2), '月'),
                     date(field0297),
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
where income <>0 or unincome<>0;

