--分区待确认收入类型
select area_nmae         大区,
       bsc_name          考核单位,
       contract_year     合同年份,
       practica_status   实施情况,
       confirm_date      周期,
       unincome_type 待确认收入类型,
       sum(income)       确认收入,
       sum(unincome)     待确认收入,
       count(distinct contract) 合同数量
from ads_gp_oa_t_finance_sales_detail_df
where date_rank=2
group by area_nmae, bsc_name, contract_year, practica_status, confirm_date,unincome_type

--总的待确认+收入
select date_year                                                                  年份,
       date_quarter                                                               季度,
       case
           when substr(to_char(confirm_date, 'YYYY-MM-DD'), 1, 4) = '2023' then 2843000000
           else 0 end                                                          as "2023总目标",

       ((cast(284300 as bigint) * cast(10000 as bigint) * 0.32) - SUM(income)) as 下季度目标,
       sum(income)                                                                确认收入,
       sum(unincome)                                                              待确认收入
from ads_gp_oa_t_finance_sales_detail_df as t1
where date_rank=2
group by date_year, date_quarter,
         case
             when substr(to_char(confirm_date, 'YYYY-MM-DD'), 1, 4) = '2023' then 2843000000
             else 0 end

--历史数据保留-到合同号-全量更新
delete from ads_gp_oa_t_finance_sales_detail_df
select * from ads_gp_oa_t_finance_sales_detail_df  limit 10
insert into ads_gp_oa_t_finance_sales_detail_df
SELECT (current_date - interval '1 days')                   dateid,    --当前存入分区
       料号                                                 item_code,
       产品名称                                             cp_name,
       大区                                                 area_nmae,
       考核单位                                             bsc_name,
       合同年份                                             contract_year,
       实施情况                                             practica_status,
       year_date as                                         date_year, --年份
       季度                                                 date_quarter,
       确认收入日期                                         confirm_date,
       DENSE_RANK() over (order by date(确认收入日期) desc) date_rank,
       待确认收入类型                                       unincome_type,
       合同                                                 contract,
       确认收入                                             income,
       待确认收入                                           unincome,
       出货量                                               purchase_qty,
       产品实际毛利                                         real_gross_profit,
       合同产品金额                                         contract_amount
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
                    concat('Q', EXTRACT('QUARTER' FROM field0297))                             季度,
             date(field0297)                                                                     确认收入日期,
             field0291                                                                           待确认收入类型,
             field0002                                                                           合同,
             sum(field0278)                                                                      确认收入,
             SUM(case when field0291 is not null then field0290 else 0 end)                      待确认收入,
             sum(case when field0066 = '产品' then field0103 else 0 end)                         出货量,
             sum(field0046)                                                                      产品实际毛利,
             sum(case when (field0029 <> '否' or field0078 <> '其他') then field0016 else 0 end) 合同产品金额
      FROM ex_ods_oa_abv5_formmain_105321 AS t1
               left join
           (select t1.大区名称, t1."三级部门ID", "NAME"
            from ex_ods_oa_abv5_saleDepartLevel as t1
                     join ex_ods_oa_abv5_org_unit as t2 on t1."三级部门ID" = CAST(t2."ID" AS TEXT)
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
                concat('Q', EXTRACT('QUARTER' FROM field0297))  ,
               date(field0297),
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
         合同产品金额



--目标：
select 大区名称 as 大区,
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
where 大区名称 in ('北分管理部', '长沙办', '上海大区', '战略合作部')






--当前日期确认收入-不用做
select case
           when 日期序号 = 1 then '本期'
           when 日期序号 = 2 then '上期'
           else null end                                      周期,
       sum(待确认收入)                                        待确认收入,
       sum(待确认收入) -
       sum(case when 日期序号 = 2 then 待确认收入 else 0 end) 本期确认收入
from (select date(field0104)                                   财务最新更新日期,
             row_number() over (order by date(field0104) desc) 日期序号,--1是最新 2是上个周期
             field0006                                         合同类型,
             field0001                                         大区,
             field0002                                         年份,
             field0004                                         合同号,
             field0010                                         合同主体,
             field0011                                         考核单位,
             field0012                                         合同客户名称,
             field0013                                         最终客户名称,
             field0014                                         付款方式,
             field0015                                         销售,
             field0016                                         实施情况,
             field0019                                         合同额,
             field0021                                         主要产品,
             field0020                                         回款比例,
             field0034                                         实施状态,
             field0035                                         实施报告回单日期,
             field0039                                         初验状态,
             field0037                                         初验预计回单日期,
             field0038                                         初验实际回单日期,
             field0042                                         终验状态,
             field0040                                         终验预计回单日期,
             field0041                                         终验实际回单日期,
             field0056                                         不含税应确认收入金额,
             field0023                                         本年确认收入,
             field0022                                         累计确认收入,
             field0024                                         待确认收入
      from ex_ods_oa_abv5_formmain_128838
      where field0057 = '是' -- and field0104 is not null  有数据后加上
     ) as t1
where (日期序号 = 1 or 日期序号 = 2)
group by case
             when 日期序号 = 1 then '本期'
             when 日期序号 = 2 then '上期'
             else null end

--总的待确认收入
select 大区,
       考核单位,
       unnest(array ['服务分摊中','已发货未确认','已下单未发货','服务期待定']) as 待确认收入类型,
       unnest(array [服务分摊中,已发货未确认,已下单未发货,服务期待定])         as values
from (select field0001      大区,
             field0011      考核单位,
             sum(coalesce(field0045,0)) 服务分摊中,
             sum(coalesce(field0043,0)) 已发货未确认,
             sum(coalesce(field0044,0)) 已下单未发货,
             sum(coalesce(field0046,0)) 服务期待定
      from ex_ods_oa_abv5_formmain_128838 as t1
      where field0104 is not null
        --and field0001 is not null
      group by field0001,
               field0011) as t




--预测完成率-整体汇总-不用做
select field0001         财务最新更新日期,

       case
           when EXTRACT('QUARTER' from current_date) = '1' then field0011
           when EXTRACT('QUARTER' from current_date) = '2' then field0017
           when EXTRACT('QUARTER' from current_date) = '3' then field0023
           when EXTRACT('QUARTER' from current_date) = '4' then field0029
           else null end 当季预测完成率,

       case
           when substr(to_char(current_date, 'YYYY-MM-DD'), 1, 4) = concat(field0004, '-04') then field0042
           when substr(to_char(current_date, 'YYYY-MM-DD'), 1, 4) = concat(field0004, '-05') then field0043
           when substr(to_char(current_date, 'YYYY-MM-DD'), 1, 4) = concat(field0004, '-06')
               then field0044 + field0045 + field0046 + field0047 + field0048
           when substr(to_char(current_date, 'YYYY-MM-DD'), 1, 4) = concat(field0004, '-07') then field0056
           when substr(to_char(current_date, 'YYYY-MM-DD'), 1, 4) = concat(field0004, '-08') then field0057
           when substr(to_char(current_date, 'YYYY-MM-DD'), 1, 4) = concat(field0004, '-09')
               then field0058 + field0059 + field0060 + field0061 + field0062
           when substr(to_char(current_date, 'YYYY-MM-DD'), 1, 4) = concat(field0004, '-10') then field0070
           when substr(to_char(current_date, 'YYYY-MM-DD'), 1, 4) = concat(field0004, '-11') then field0071
           when substr(to_char(current_date, 'YYYY-MM-DD'), 1, 4) = concat(field0004, '-12')
               then field0072 + field0073 + field0074 + field0075 + field0076
           else null end 当月预测收入,

       case
           when substr(to_char(current_date, 'YYYY-MM-DD'), 1, 4) = concat(field0004, '-04') then field0035
           when substr(to_char(current_date, 'YYYY-MM-DD'), 1, 4) = concat(field0004, '-05') then field0036
           when substr(to_char(current_date, 'YYYY-MM-DD'), 1, 4) = concat(field0004, '-06')
               then field0037 + field0038 + field0039 + field0040 + field0041
           when substr(to_char(current_date, 'YYYY-MM-DD'), 1, 4) = concat(field0004, '-07') then field0049
           when substr(to_char(current_date, 'YYYY-MM-DD'), 1, 4) = concat(field0004, '-08') then field0050
           when substr(to_char(current_date, 'YYYY-MM-DD'), 1, 4) = concat(field0004, '-09')
               then field0051 + field0052 + field0053 + field0054 + field0055
           when substr(to_char(current_date, 'YYYY-MM-DD'), 1, 4) = concat(field0004, '-10') then field0063
           when substr(to_char(current_date, 'YYYY-MM-DD'), 1, 4) = concat(field0004, '-11') then field0064
           when substr(to_char(current_date, 'YYYY-MM-DD'), 1, 4) = concat(field0004, '-12')
               then field0065 + field0066 + field0067 + field0068 + field0069
           else null end 当月实际收入
from ex_ods_oa_abv5_formmain_129544



----预测完成率-分区-不用做
select field0001         财务最新更新日期,
       field0002         大区,
       case
           when EXTRACT('QUARTER' from current_date) = '1' then field0008
           when EXTRACT('QUARTER' from  current_date) = '2' then field0014
           when EXTRACT('QUARTER' from  current_date) = '3' then field0020
           when EXTRACT('QUARTER' from  current_date) = '4' then field0026
           else null end 当前目标,
       case
           when EXTRACT('QUARTER'from current_date) = '1' then field0009
           when EXTRACT('QUARTER'from current_date) = '2' then field0015
           when EXTRACT('QUARTER'from current_date) = '3' then field0021
           when EXTRACT('QUARTER'from current_date) = '4' then field0027
           else null end 当前预测完成,

       case
           when EXTRACT('QUARTER'from current_date) = '1' then field0010
           when EXTRACT('QUARTER'from current_date) = '2' then field0016
           when EXTRACT('QUARTER'from current_date) = '3' then field0022
           when EXTRACT('QUARTER'from current_date) = '4' then field0028
           else null end 当前已完成收入
from ex_ods_oa_abv5_formmain_129544



