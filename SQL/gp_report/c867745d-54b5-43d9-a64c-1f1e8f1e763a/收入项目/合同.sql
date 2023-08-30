--重点项目聚合
select 大区,
       实施情况,
       case
           when 日期序号 = 1 then '本期'
           when 日期序号 = 2 then '上期'
           else null end                                                         周期,
       sum(待确认收入)                                                           待确认收入,
       sum(本年确认收入)                                                         已确认收入,
       count(distinct 合同号)                                                    合同数,
       count(distinct case when 终验状态 = '终验完成' then 合同号 else null end) 验收完成合同数
--        count(distinct case when 日期序号 = 2 then 合同号 else null end)                           as 上期合同数,
--        count(distinct case when 日期序号 = 1 and 终验状态 = '终验完成' then 合同号 else null end) as 本期验收完成合同数,
--        count(distinct case when 日期序号 = 1 and 终验状态 = '终验完成' then 合同号 else null end) /
--        nullif(count(distinct case when 日期序号 = 2 then 合同号 else null end), 0)                as 验收完成率,
--        sum(case when 日期序号 = 2 then 待确认收入 else 0 end)                                        上期待确认收入,
--        (sum(case when 日期序号 = 1 then 待确认收入 else 0 end) -
--         sum(case when 日期序号 = 2 then 待确认收入 else 0 end))                                      本期确认收入_图
from ads_gp_oa_t_finance_contract_detail_df as t1
where (日期序号 = 1 or 日期序号 = 2)
group by 大区,
         实施情况,
         case
             when 日期序号 = 1 then '本期'
             when 日期序号 = 2 then '上期'
             else null end


--重点项目合同明细
select case
           when 日期序号 = 1 then '本期'
           when 日期序号 = 2 then '上期'
           end 周期,
       *
from ads_gp_oa_t_finance_contract_detail_df  as t
where (日期序号 = 1 or 日期序号 = 2)



--历史数据留存
--select * from ads_gp_oa_t_finance_contract_detail_df
delete from ads_gp_oa_t_finance_contract_detail_df;
insert into ads_gp_oa_t_finance_contract_detail_df
select (current_date - interval '1 days') dateid,  --写入日期
       date(t1.field0118)                 财务最新更新日期,
       1                                  日期序号,--1是最新 2是上个周期
       t1.field0006                       合同类型,
       t1.field0001                       大区,
       t1.field0002                       年份,
       t1.field0004                       合同号,
       t1.field0010                       合同主体,
       t1.field0011                       考核单位,
       t1.field0012                       合同客户名称,
       t1.field0013                       最终客户名称,
       t1.field0014                       付款方式,
       t1.field0045                       服务分摊,
       t1.field0043                       已发货未确认,
       t1.field0044                       已下单未发货,
       t1.field0046                       服务期待定,
       t1.field0109                       待确认收入类型,
       t1.field0015                       销售,
       t1.field0016                       实施情况,
       t1.field0019                       合同额,
       t1.field0021                       主要产品,
       t1.field0020                       回款比例,
       t3."SHOWVALUE"                     实施状态,
       t1.field0035                       实施报告回单日期,
       t9."SHOWVALUE"                     初验状态,
       t1.field0037                       初验预计回单日期,
       t1.field0038                       初验实际回单日期,
       t4."SHOWVALUE"                     终验状态,
       t1.field0040                       终验预计回单日期,
       t1.field0041                       终验实际回单日期,
       t1.field0056                       不含税应确认收入金额,
       t1.field0023                       本年确认收入,
       t1.field0022                       累计确认收入,
       t1.field0024                       待确认收入,
       t1.field0105                       收入未确认原因,
        t5.field0006                      项目经理,
       t6."NAME"                          项目经理所在部门,
       t7."SHOWVALUE"                     是否全部发货,
       t8."SHOWVALUE"                     是否有初验,
       t1.field0030                       到货证明计划签署日期,
       t1.field0032                       实施报告计划签署日期,
       t1.field0033                       服务确认单计划签署日期,
       t1.field0037                       初验报告计划签署日期,
       t1.field0040                       终验报告计划签署日期,
       t1.field0097                       代理商到货证明时间,
       t1.field0095                       最终用户到货证明时间,
       t1.field0098                       代理商实施报告时间,
       t1.field0096                       最终用户实施报告时间,
       t1.field0100                       代理商服务确认单时间,
       t1.field0036                       最终用户服务确认单时间,
       t1.field0099                       代理商验收报告时间,
       t1.field0041                       最终用户验收报告时间,
       t1.field0047                       是否已确认收入,
      tt1."SHOWVALUE"                  是否终验,
        tt3."SHOWVALUE"                     回单当季是否可拿回,--
       t1.field0050                       回单拿回时间,
       t1.field0051                       预计回款时间,
       t1.field0052		预计回款金额,
       t1.field0053                       是否需要总部资源,
       t1.field0054                       备注,
      tt2."SHOWVALUE"                    是否符合当季收入,
       t1.field0057                       超百万项目,
       t1.field0072                       合同审批完成时间,
       t1.field0118                       收入确认截止日期,
       t1.field0017                       销售方式,
       t1.field0018                       一级行业,
       t1.field0005                       合同状态
from ex_ods_oa_abv5_formmain_128838 as t1
         left join ex_ods_oa_abv5_ctp_enum_item t3 on cast(t1.field0034 as text) = cast(t3."ID" as text)
         left join ex_ods_oa_abv5_ctp_enum_item t4 on cast(t1.field0042 as text) = cast(t4."ID" as text)
         left join ex_ods_oa_abv5_formmain_10037 t5 on cast(t1.field0026 as text) = cast(t5.field0175 as text)
         left join ex_ods_oa_abv5_org_unit t6 on cast(t1.field0027 as text) = cast(t6."ID" as text)
         left join ex_ods_oa_abv5_ctp_enum_item t7 on cast(t1.field0028 as text) = cast(t7."ID" as text)
         left join ex_ods_oa_abv5_ctp_enum_item t8 on cast(t1.field0029 as text) = cast(t8."ID" as text)
         left join ex_ods_oa_abv5_ctp_enum_item t9 on cast(t1.field0039 as text) = cast(t9."ID" as text)
left join ex_ods_oa_abv5_ctp_enum_item tt1 on cast(t1.field0048 as text) = cast(tt1."ID" as text)
left join ex_ods_oa_abv5_ctp_enum_item tt2 on cast(t1.field0055 as text) = cast(tt2."ID" as text)
left join ex_ods_oa_abv5_ctp_enum_item  tt3 on  cast(t1.field0049 as text) = cast(tt3."ID" as text)
where t1.field0057 = '是'
  and t1.field0104 is not null
--有数据后加上



--重点项目收入类型明细
select
       date(field0104)                                                            财务最新更新日期,
       field0006                                                                  合同类型,
       field0001                                                                  大区,
       field0002                                                                  年份,
       field0004                                                                  合同号,
       field0010                                                                  合同主体,
       field0011                                                                  考核单位,
       field0012                                                                  合同客户名称,
       field0013                                                                  最终客户名称,
       field0014                                                                  付款方式,
       unnest(array ['服务分摊中','已发货未确认','已下单未发货','服务期待定']) as 待确认收入类型,
       unnest(array [field0045,field0043,field0044,field0046])                 as values
from ex_ods_oa_abv5_formmain_128838 as t1
where field0057 = '是'
  and field0104 is not null


-- 3177462661668880692  未终验
-- -818035817598399797 终验中
-- 454312138872037582 终验完成
