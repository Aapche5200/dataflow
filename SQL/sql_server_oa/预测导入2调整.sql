--增加收入合计结果表
select t1.field0001,
       field0002,
       field0003,
       field0004,
       收入合计                                                         as field0006,
       field0007,
       收入合计                                                         as field0010,
       t2.全年收入目标 * 10000 * 0.128                                  as field0008,--Q1收入目标
       round(field0010 / nullif(t2.全年收入目标 * 10000 * 0.128, 0), 4) as field0012,--Q1收入完成率,
       t2.全年收入目标 * 10000                                          as field0005,
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
      from (SELECT (GETDATE() - 1)                                  dateid,    --当前存入分区
                   料号                                             item_code,
                   产品名称                                         cp_name,
                   大区                                             area_nmae,
                   考核单位                                         bsc_name,
                   合同年份                                         contract_year,
                   实施情况                                         practica_status,
                   year_date as                                     date_year, --年份
                   季度                                             date_quarter,
                   确认收入日期                                     confirm_date,
                   row_number() over (order by (确认收入日期) desc) date_rank,
                   待确认收入类型                                   unincome_type,
                   合同                                             contract,
                   确认收入                                         income,
                   待确认收入                                       unincome,
                   出货量                                           purchase_qty,
                   产品实际毛利                                     real_gross_profit,
                   合同产品金额                                     contract_amount
            from (SELECT field0004                                                                           料号,
                         大区名称                                                                            大区,
                         case
                             when field0012 = '北京渠道事业部' then '渠道事业部(北京分公司)'
                             when field0012 = '军民融合事业部二部' then '军民融合事业二部'
                             else field0012 end                                                              考核单位,
                         field0067                                                                           产品名称,
                         field0108                                                                           合同年份,
                         field0049                                                                           实施情况,
                         datepart(year, field0297)                                                           year_date,
                         concat(datepart(year, field0297),
                                concat('Q', datepart(quarter, field0297)))                                   季度,
                         convert(varchar(100), field0297,23)                                                        确认收入日期,
                         field0291                                                                           待确认收入类型,
                         field0002                                                                           合同,
                         sum(field0278)                                                                      确认收入,
                         SUM(case when field0291 is not null then field0290 else 0 end)                      待确认收入,
                         sum(case when field0066 = '产品' then field0103 else 0 end)                         出货量,
                         sum(field0046)                                                                      产品实际毛利,
                         sum(case when (field0029 <> '否' or field0078 <> '其他') then field0016 else 0 end) 合同产品金额
                  FROM formmain_105321 AS t1
                           left join
                       (select t1.大区名称, t1."三级部门ID", "NAME"
                        from saleDepartLevel as t1
                                 join org_unit as t2 on t1."三级部门ID" = t2."ID"
                        where t1.大区名称 in ('北分管理部', '北区管理部', '南区管理部', '战略合作部', '长沙办', '上海大区')
                        group by t1.大区名称, t1."三级部门ID", "NAME") as t2 on case
                                                                                    when field0012 = '北京渠道事业部'
                                                                                        then '渠道事业部(北京分公司)'
                                                                                    when field0012 = '军民融合事业部二部'
                                                                                        then '军民融合事业二部'
                                                                                    else field0012 end = t2."NAME"
                  GROUP BY field0004, 大区名称,
                           case
                               when field0012 = '北京渠道事业部' then '渠道事业部(北京分公司)'
                               when field0012 = '军民融合事业部二部' then '军民融合事业二部'
                               else field0012 end, field0067,
                           field0108,
                           field0049, datepart(year, field0297),
                           concat(datepart(year, field0297),
                                  concat('Q', datepart(quarter, field0297))),
                           convert(varchar(100), field0297,23),
                           field0291, field0002) as t
            group by 料号,
                     产品名称,
                     大区,
                     考核单位,
                     合同年份,
                     实施情况,
                     year_date, --年份
                     季度,
                     确认收入日期,
                     待确认收入类型,
                     合同,
                     确认收入,
                     待确认收入,
                     出货量,
                     产品实际毛利,
                     合同产品金额) AS T
      where area_nmae is not null
      group by confirm_date, area_nmae, bsc_name, date_year) as t1
         left join (select 大区名称 as 大区,
                           "NAME"   as 办事处,
                           全年收入目标
                    from (select 大区名称, "三级部门ID", sum(field0044) 全年收入目标
                          from formmain_5137 as t1
                                   left join saleDepartLevel as t2 on t1.field0002 = t2."销售部门ID"
                          where 大区名称 in ('北区管理部', '南区管理部')
                            and t1.field0004 = 2023
                            and t1.field0007 = '8559535201370522547'
                            and t1.field0044 is not null
                          group by 大区名称, "三级部门ID") as t1
                             join org_unit as t2 on t1."三级部门ID" = t2."ID"

                    union all

                    SELECT 大区名称,
                           case when "NAME" = '合肥办' then '安徽办' else "NAME" END name,
                           全年收入目标
                    FROM (select field0002, "NAME", sum(field0044) 全年收入目标
                          from formmain_5137 as t1
                                   join org_unit as t2 on t1.field0002 = t2."ID"
                          WHERE field0004 = 2023
                            and field0007 = '8559535201370522547'
                            and field0044 is not null
                          group by field0002, "NAME") AS t1
                             join saleDepartLevel as t2 on t1.field0002 = t2."销售部门ID"
                    where 大区名称 in ('北分管理部', '长沙办', '上海大区', '战略合作部')) as t2
                   on t1.field0002 = t2.大区 and t1.field0003 = t2.办事处
       left  join
     (select 大区名称                  as 大区,
             case
                 when fm.field0006 = '北京渠道事业部' then '渠道事业部(北京分公司)'
                 when fm.field0006 = '军民融合事业部二部' then '军民融合事业二部'
                 else fm.field0006 end    考核单位,
             sum(fm.field0057) * 10000 as 收入合计
      from formmain_105322 as fm
               left join (select t1.大区名称, t1."三级部门ID", "NAME"
                          from saleDepartLevel as t1
                                   join org_unit as t2 on t1."三级部门ID" = t2."ID"
                          where t1.大区名称 in ('北分管理部', '北区管理部', '南区管理部', '战略合作部', '长沙办', '上海大区')
                          group by t1.大区名称, t1."三级部门ID", "NAME") as t2
                         on case
                                when fm.field0006 = '北京渠道事业部' then '渠道事业部(北京分公司)'
                                when fm.field0006 = '军民融合事业部二部' then '军民融合事业二部'
                                else fm.field0006 end = t2."NAME"
      where 大区名称 is not null
      group by 大区名称,
               case
                   when fm.field0006 = '北京渠道事业部' then '渠道事业部(北京分公司)'
                   when fm.field0006 = '军民融合事业部二部' then '军民融合事业二部'
                   else fm.field0006 end) as t3 on t1.field0002 = t3.大区 and t1.field0003 = t3.考核单位





