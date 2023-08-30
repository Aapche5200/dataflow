--商密商机中间表设计
with tmp as (SELECT   --商密产品
fm.field0003 as 商机编码,
--isnull(cei."SHOWVALUE",cei1."SHOWVALUE") as 产品是否商密,
cei."SHOWVALUE" as 模块商密信息,
cei."SHOWVALUE" as 型号商密信息,
fs.field0045 as 产品预计成交数量,
fs.field0046 as 产品预计成交金额,
fs.field0084 as 预计成交单价,
fm.field0104 as 创建日期,
fm.field0103 as 最后更新时间
FROM ex_ods_oa_abv5_formmain_2156 fm
left join ex_ods_oa_abv5_formson_2157 fs on cast(fs.formmain_id as text)=cast(fm."ID"  as text)
left join ex_ods_oa_abv5_formmain_1294 fm_12 on cast(fs.field0097 as text)=cast(fm_12.field0008 as text)
left join ex_ods_oa_abv5_formmain_125708 fm_125 on cast(fs.field0097 as text)=cast(fm_125.field0009 as text)
left join ex_ods_oa_abv5_CTP_ENUM_ITEM cei on cast(cei."ID" as text)=cast(fm_12.field0165 as text)
left join ex_ods_oa_abv5_CTP_ENUM_ITEM cei1 on cast(cei1."ID" as text)=cast(fm_125.field0033 as text)
WHERE
fm.field0028='-4552238618822292475' --项目状态‘进行中’
AND(cei."SHOWVALUE"='是' or cei1."SHOWVALUE"='是')
--and left(fm.field0019,4)>=2022
--AND isnull(cei.SHOWVALUE,cei1.SHOWVALUE)='是' --产品是否商密‘是’
--AND DATEDIFF(day,fm.field0018,'2007-01-01')<=0 AND DATEDIFF(day,fm.field0018,'2100-01-01')>=0),
and fm.field0018>'2007-01-01'),
tmp1 as (--商密商机金额
   select 商机编码,sum(产品预计成交金额) as 商机金额
   from tmp group by 商机编码),
tmp2 as (---商密商机
 SELECT
field0003 as 商机编码,
field0004 as 客户名称,
--(select NAME from ex_ods_oa_abv5_ORG_MEMBER where cast("ID" as text)=cast(field0039 as text) as 销售人员,
--(select NAME from ex_ods_oa_abv5_ORG_UNIT where cast("ID" as text)=cast(field0040 as text) as 销售单位,
t2."NAME" as 销售人员,
t3."NAME" as 销售单位,
field0002 as 商机名称,
--(select SHOWVALUE from ex_ods_oa_abv5_CTP_ENUM_ITEM where cast("ID" as text)=cast(field0028 as text) as 项目状态,
--(select SHOWVALUE from ex_ods_oa_abv5_CTP_ENUM_ITEM where cast("ID" as text)=cast(field0016 as text) as 销售阶段,
t4."SHOWVALUE" as 项目状态,
t5."SHOWVALUE" as 销售阶段,
field0017 as 赢单率,
field0018 as 预计成交时间,
field0019 as 预计成交季度,
case when field0019='2023.Q1' then '202301'
     when field0019='2023.Q2' then '202302'
     when field0019='2023.Q3' then '202303'
     when field0019='2023.Q4' then '202304'
     when field0019='2024.Q1' then '202401'
     when field0019='2024.Q2' then '202402'
     when field0019='2024.Q3' then '202403'
     when field0019='2024.Q4' then '202404'
     when field0019='2025.Q1' then '202501'
     when field0019='2025.Q2' then '202502'
     when field0019='2025.Q3' then '202503'
     when field0019='2025.Q4' then '202504'
     end 预计成交季度_排序,
--(select SHOWVALUE from ex_ods_oa_abv5_CTP_ENUM_ITEM where cast("ID" as text)=cast(field0037 as text) as 已计入FC,
--(select SHOWVALUE from ex_ods_oa_abv5_CTP_ENUM_ITEM where cast("ID" as text)=cast(field0160 as text) as 一级行业,
--(select SHOWVALUE from ex_ods_oa_abv5_CTP_ENUM_ITEM where cast("ID" as text)=cast(field0161 as text) as 二级行业,
--(select SHOWVALUE from ex_ods_oa_abv5_CTP_ENUM_ITEM where cast("ID" as text)=cast(field0282 as text) as 三级行业,
t6."SHOWVALUE" as 已计入FC,
t7."SHOWVALUE" as 一级行业,
t8."SHOWVALUE" as 二级行业,
t9."SHOWVALUE" as 三级行业,
field0104 as 创建日期,
field0103 as 最后更新时间,
'是' as 是否商密
fROM ex_ods_oa_abv5_formmain_2156 as t1
left join ex_ods_oa_abv5_ORG_MEMBER as t2 on cast(t2."ID" as text)=cast(t1.field0039 as text)
left join ex_ods_oa_abv5_ORG_UNIT as t3 on cast(t3."ID" as text)=cast(t1.field0040 as text)
left join ex_ods_oa_abv5_CTP_ENUM_ITEM as t4 on cast(t4."ID" as text)=cast(t1.field0028 as text)
left join ex_ods_oa_abv5_CTP_ENUM_ITEM as t5 on cast(t5."ID" as text)=cast(t1.field0016 as text)
left join ex_ods_oa_abv5_CTP_ENUM_ITEM as t6 on cast(t6."ID" as text)=cast(t1.field0037 as text)
left join ex_ods_oa_abv5_CTP_ENUM_ITEM as t7 on cast(t7."ID" as text)=cast(t1.field0160 as text)
left join ex_ods_oa_abv5_CTP_ENUM_ITEM as t8 on cast(t8."ID" as text)=cast(t1.field0161 as text)
left join ex_ods_oa_abv5_CTP_ENUM_ITEM as t9 on cast(t9."ID" as text)=cast(t1.field0282 as text)
where field0003 in (select distinct 商机编码 from tmp))
select tmp2.*,tmp1.商机金额 from tmp2 left join tmp1 on tmp2.商机编码=tmp1.商机编码
order by 创建日期 desc
;