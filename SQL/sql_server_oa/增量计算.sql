--插入计算
insert into ads_gp_oa_t_area_unit_sales_dd
select *
from (select t1.date_y,
             t1.date_q,
             t1.date_m,
             t1.confirm_date,
             t1.area,
             t1.area_unit,
             IIF(t1.income - t2.current_income = null, 0, t1.income - t2.current_income)         as income,
             IIF(t1.unincome - t2.current_unincome = null, 0, t1.unincome - t2.current_unincome) as unincome,
             t1.update_date
      from (select year_date                  date_y,
                   季度                       date_q,
                   date_m,
                   确认收入日期               confirm_date,
                   更新日期                   update_date,
                   case
                       when 考核单位 = '全国运营商行业部' then '其它（营销中心）'
                       when 考核单位 in ('北京网安', '金华总经办', '数瀚项目实施部', '综合业务部')
                           then '其它（非营销中心）'
                       when 大区 is null then '其他'
                       else 大区 end          area,
                   考核单位                   area_unit,
                   round(sum(确认收入), 0) as income,
                   round(sum(待确认收入), 0)  unincome
            from (SELECT field0004                                                                           料号,
                         大区名称                                                                            大区,
                         case
                             when field0012 = '北京渠道事业部' then '渠道事业部(北京分公司)'
                             when field0012 = '军民融合事业部二部' then '军民融合事业二部'
                             when field0012 = '上海浦东新监管事业部' then '上海新监管浦东事业部'
                             else field0012 end                                                              考核单位,
                         field0067                                                                           产品名称,
                         field0108                                                                           合同年份,
                         field0049                                                                           实施情况,
                         substring(cast(convert(varchar(100), field0297, 120) as varchar(100)), 1, 4)        year_date,
                         concat('Q', datepart(quarter, field0297))                                           季度,
                         concat(datepart(month, field0297), '月')                                            date_m,
                         convert(varchar(100), field0297, 23)                                                确认收入日期,
                         convert(varchar(100), field0009, 23)                                                更新日期,
                         field0291                                                                           待确认收入类型,
                         field0002                                                                           合同,
                         sum(field0278)                                                                      确认收入,
                         SUM(field0290)                      待确认收入,
                         sum(case when field0066 = '产品' then field0103 else 0 end)                         出货量,
                         sum(field0046)                                                                      产品实际毛利,
                         sum(case when (field0029 <> '否' or field0078 <> '其他') then field0016 else 0 end) 合同产品金额
                  FROM formmain_105321 AS t1
                           left join
                       (select t1.大区名称, t1."三级部门ID", "NAME"
                        from saleDepartLevel as t1
                                 join org_unit as t2 on t1."三级部门ID" = t2."ID"
                        where t1.大区名称 in ('北分管理部', '北区管理部', '南区管理部', '战略合作部', '长沙办', '上海大区')
                        group by t1.大区名称, t1."三级部门ID", "NAME") as t2
                       on case
                              when field0012 = '北京渠道事业部' then '渠道事业部(北京分公司)'
                              when field0012 = '军民融合事业部二部' then '军民融合事业二部'
                              when field0012 = '上海浦东新监管事业部' then '上海新监管浦东事业部'
                              else field0012 end = t2."NAME"
                           join (select field0002 as contract_code
                                 from formmain_1119
                                 where coalesce(field1039, '其他') <> '已注销') as t3 on t1.field0002 = t3.contract_code
                  where coalesce(t1.field0027, '其他') <> '内部结算'
                  GROUP BY field0004, 大区名称,
                           case
                               when field0012 = '北京渠道事业部' then '渠道事业部(北京分公司)'
                               when field0012 = '军民融合事业部二部' then '军民融合事业二部'
                               when field0012 = '上海浦东新监管事业部' then '上海新监管浦东事业部'
                               else field0012 end, field0067,
                           field0108,
                           field0049,
                           substring(cast(convert(varchar(100), field0297, 120) as varchar(100)), 1, 4),
                           concat('Q', datepart(quarter, field0297)),
                           concat(datepart(month, field0297), '月'),
                           convert(varchar(100), field0297, 23), convert(varchar(100), field0009, 23),
                           field0291, field0002) as t1
                 --where 大区 is not null
            group by year_date,
                     季度,
                     date_m,
                     确认收入日期,
                     更新日期,
                     case
                         when 考核单位 = '全国运营商行业部' then '其它（营销中心）'
                         when 考核单位 in ('北京网安', '金华总经办', '数瀚项目实施部', '综合业务部')
                             then '其它（非营销中心）'
                          when 大区 is null then '其他'
                         else 大区 end,
                     考核单位) as t1
               left join (select cast(area as varchar(800))                as area,
                                 cast(area_unit as varchar(800))           as area_unit,
                                 cast(date_y as varchar(800))              as date_y,
                                 max_date                                  as confirm_date,
                                 max_update_date                           as update_date,
                                 round(sum(coalesce(income_diff, 0)), 0)   as current_income,
                                 round(sum(coalesce(unincome_diff, 0)), 0) as current_unincome
                          from ads_gp_oa_t_area_unit_sales_dd as t
                                   left join (select max(confirm_date) max_date, max(update_date) max_update_date
                                              from ads_gp_oa_t_area_unit_sales_dd) as t2 on 1 = 1
                          group by cast(area as varchar(800))
                                 , cast(area_unit as varchar(800))
                                 , cast(date_y as varchar(800))
                                 , max_date, max_update_date) as t2
                         on t1.date_y = t2.date_y and t1.area = t1.area and t1.area_unit = t2.area_unit and
                            t1.update_date <> t2.update_date) as m
where income <> 0
   or unincome <> 0;