--  q1总的数据导入
select *  from ads_gp_oa_t_area_unit_sales_dd where 确认日期=date('2023-04-21')
create table ads_gp_oa_t_area_unit_sales_dd as
select t1.field0004                                      年份,
       concat('Q', EXTRACT('QUARTER' FROM t1.field0001)) 季度,
       concat(EXTRACT('month' FROM t1.field0001), '月')  月度,
       t1.field0001                                      确认日期,
       t1.field0002                                      大区,
       t1.field0003                                      考核单位,
       field0006                                         年度累计确认收入差值,
       field0007                                         当前待确认收入差值
from (select confirm_date as field0001,--确认日期
             area_nmae       field0002,--大区,
             bsc_name        field0003,-- 办事处,
             date_year       field0004,--年份
             sum(income)     field0006,--  年度累计确认收入,
             sum(unincome)   field0007,--  当前待确认收入,
             --sum(income)     field0008,--Q1收入目标
             sum(income)     field0010--Q1完成收入
      from ads_gp_oa_t_finance_sales_detail_df
      where area_nmae is not null
        and date_rank = 1
      group by confirm_date, area_nmae, bsc_name, date_year) as t1
         left join (select 大区名称 as 大区,
                           "NAME"   as 办事处,
                           全年收入目标
                    from (select 大区名称, "三级部门ID", sum(field0044) 全年收入目标
                          from ex_ods_oa_abv5_formmain_5137 as t1
                                   left join ex_ods_oa_abv5_saleDepartLevel as t2 on t1.field0002 = t2."销售部门ID"
                          where 大区名称 in ('北区管理部', '南区管理部')
                            and t1.field0004 = 2023
                            and t1.field0007 = '8559535201370522547'
                            and t1.field0044 is not null
                          group by 大区名称, "三级部门ID") as t1
                             join ex_ods_oa_abv5_org_unit as t2 on t1."三级部门ID" = CAST(t2."ID" AS TEXT)

                    union all

                    SELECT 大区名称,
                           case when "NAME" = '合肥办' then '安徽办' else "NAME" END name,
                           全年收入目标
                    FROM (select field0002, "NAME", sum(field0044) 全年收入目标
                          from ex_ods_oa_abv5_formmain_5137 as t1
                                   join ex_ods_oa_abv5_org_unit as t2 on t1.field0002 = CAST(t2."ID" AS TEXT)
                          WHERE field0004 = 2023
                            and field0007 = '8559535201370522547'
                            and field0044 is not null
                          group by field0002, "NAME") AS t1
                             join ex_ods_oa_abv5_saleDepartLevel as t2 on t1.field0002 = t2."销售部门ID"
                    where 大区名称 in ('北分管理部', '长沙办', '上海大区', '战略合作部')) as t2
                   on t1.field0002 = t2.大区 and t1.field0003 = t2.办事处

--以后历史相减导入
insert into ads_gp_oa_t_area_unit_sales_dd
select t1.field0004                                          年份,
       concat('Q', EXTRACT('QUARTER' FROM t1.field0001))     季度,
       concat(EXTRACT('month' FROM t1.field0001), '月')      月度,
       t1.field0001                                          确认日期,
       t1.field0002                                          大区,
       t1.field0003                                          考核单位,
       COALESCE(t1.field0006, 0) - COALESCE(t2.field0006, 0) 年度累计确认收入差值,
       COALESCE(t1.field0007, 0) - COALESCE(t2.field0007, 0) 当前待确认收入差值
from (select confirm_date as field0001,--确认日期
             area_nmae       field0002,--大区,
             bsc_name        field0003,-- 办事处,
             date_year       field0004,--年份
             sum(income)     field0006,--  年度累计确认收入,
             sum(unincome)   field0007,--  当前待确认收入,
             --sum(income)     field0008,--Q1收入目标
             sum(income)     field0010--Q1完成收入
      from ads_gp_oa_t_finance_sales_detail_df
      where area_nmae is not null
        and date_rank =  (select max(date_rank) from ads_gp_oa_t_finance_sales_detail_df)
      group by confirm_date, area_nmae, bsc_name, date_year) as t1
         join
     (select t1.*,
             t2.全年收入目标 * 10000 * 0.128                                     field0008,--Q1目标
             round(field0010 / nullif(t2.全年收入目标 * 10000 * 0.128, 0), 4) as field0012,--Q1收入完成率,
             t2.全年收入目标 * 10000                                          as field0005, --年度收入目标
             (t2.全年收入目标 * 10000 * 0.32 - field0010)                     as field0014, --Q2收入目标
             (row_number() over (order by field0003)) + 1000000000000000      as ID
      from (select confirm_date as field0001,--确认日期
                   area_nmae       field0002,--大区,
                   bsc_name        field0003,-- 办事处,
                   date_year       field0004,--年份
                   sum(income)     field0006,--  年度累计确认收入,
                   sum(unincome)   field0007,--  当前待确认收入,
                   --sum(income)     field0008,--Q1收入目标
                   sum(income)     field0010--Q1完成收入
            from ads_gp_oa_t_finance_sales_detail_df as a
            where area_nmae is not null
              and date_rank =  (select max(date_rank)-1 from ads_gp_oa_t_finance_sales_detail_df)
            group by confirm_date, area_nmae, bsc_name, date_year) as t1
               left join (select 大区名称 as 大区,
                                 "NAME"   as 办事处,
                                 全年收入目标
                          from (select 大区名称, "三级部门ID", sum(field0044) 全年收入目标
                                from ex_ods_oa_abv5_formmain_5137 as t1
                                         left join ex_ods_oa_abv5_saleDepartLevel as t2 on t1.field0002 = t2."销售部门ID"
                                where 大区名称 in ('北区管理部', '南区管理部')
                                  and t1.field0004 = 2023
                                  and t1.field0007 = '8559535201370522547'
                                  and t1.field0044 is not null
                                group by 大区名称, "三级部门ID") as t1
                                   join ex_ods_oa_abv5_org_unit as t2 on t1."三级部门ID" = CAST(t2."ID" AS TEXT)

                          union all

                          SELECT 大区名称,
                                 case when "NAME" = '合肥办' then '安徽办' else "NAME" END name,
                                 全年收入目标
                          FROM (select field0002, "NAME", sum(field0044) 全年收入目标
                                from ex_ods_oa_abv5_formmain_5137 as t1
                                         join ex_ods_oa_abv5_org_unit as t2 on t1.field0002 = CAST(t2."ID" AS TEXT)
                                WHERE field0004 = 2023
                                  and field0007 = '8559535201370522547'
                                  and field0044 is not null
                                group by field0002, "NAME") AS t1
                                   join ex_ods_oa_abv5_saleDepartLevel as t2 on t1.field0002 = t2."销售部门ID"
                          where 大区名称 in ('北分管理部', '长沙办', '上海大区', '战略合作部')) as t2
                         on t1.field0002 = t2.大区 and t1.field0003 = t2.办事处) as t2
     on t1.field0002 = t2.field0002 and t1.field0003 = t2.field0003 and t1.field0004 = t2.field0004


select * from ads_gp_oa_t_finance_sales_detail_df limit 10