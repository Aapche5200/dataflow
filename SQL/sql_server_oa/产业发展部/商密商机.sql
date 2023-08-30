--商密商机明细
with tmp as (SELECT --商密产品
                    fm.field0003                          as 商机编码,
                    isnull(cei.SHOWVALUE, cei1.SHOWVALUE) as 产品是否商密,
                    fs.field0045                          as 产品预计成交数量,
                    fs.field0046                          as 产品预计成交金额,
                    fs.field0084                          as 预计成交单价,
                    fm.field0104                          as 创建日期,
                    fm.field0103                          as 最后更新时间
             FROM formmain_2156 fm
                      left join formson_2157 fs on fs.formmain_id = fm.id
                      left join formmain_1294 fm_12 on fs.field0097 = fm_12.field0008
                      left join formmain_125708 fm_125 on fs.field0097 = fm_125.field0009
                      left join CTP_ENUM_ITEM cei on cei.id = fm_12.field0165
                      left join CTP_ENUM_ITEM cei1 on cei1.id = fm_125.field0033
             WHERE fm.field0028 = '-4552238618822292475'        --项目状态‘进行中’
               and substring(fm.field0019, 1, 4) >= '2022'
               AND isnull(cei.SHOWVALUE, cei1.SHOWVALUE) = '是' --产品是否商密‘是’
               AND DATEDIFF(day, fm.field0018, '2007-01-01') <= 0
               AND DATEDIFF(day, fm.field0018, '2100-01-01') >= 0),
     tmp1 as (--商密商机金额
         select 商机编码, sum(产品预计成交金额) as 商机金额
         from tmp
         group by 商机编码),
     tmp2 as (---商密商机
         SELECT fm.field0003 as 商机编码,
                fm.field0004 as 客户名称,
                t1.NAME      as 销售人员,
                t2.NAME      as 销售单位,
                fm.field0002 as 商机名称,
                t3.SHOWVALUE as 项目状态,
                t4.SHOWVALUE as 销售阶段,
                fm.field0017 as 赢单率,
                fm.field0018 as 预计成交时间,
                fm.field0019 as 预计成交季度,
                case
                    when fm.field0019 = '2023.Q1' then '202301'
                    when fm.field0019 = '2023.Q2' then '202302'
                    when fm.field0019 = '2023.Q3' then '202303'
                    when fm.field0019 = '2023.Q4' then '202304'
                    when fm.field0019 = '2024.Q1' then '202401'
                    when fm.field0019 = '2024.Q2' then '202402'
                    when fm.field0019 = '2024.Q3' then '202403'
                    when fm.field0019 = '2024.Q4' then '202404'
                    when fm.field0019 = '2025.Q1' then '202501'
                    when fm.field0019 = '2025.Q2' then '202502'
                    when fm.field0019 = '2025.Q3' then '202503'
                    when fm.field0019 = '2025.Q4' then '202504'
                    end         预计成交季度_排序,
                t5.SHOWVALUE as 已计入FC,
                t6.SHOWVALUE as 一级行业,
                t7.SHOWVALUE as 二级行业,
                t8.SHOWVALUE as 三级行业,
                fm.field0104 as 创建日期,
                fm.field0103 as 最后更新时间,
                '是'         as 是否商密
         fROM formmain_2156 as fm
                  left join ORG_MEMBER as t1 on t1.id = fm.field0039
                  left join ORG_UNIT as t2 on t2.ID = fm.field0040
                  left join CTP_ENUM_ITEM as t3 on t3.ID = fm.field0028
                  left join CTP_ENUM_ITEM as t4 on t4.ID = fm.field0016
                  left join CTP_ENUM_ITEM as t5 on t5.ID = fm.field0037
                  left join CTP_ENUM_ITEM as t6 on t6.ID = fm.field0160
                  left join CTP_ENUM_ITEM as t7 on t7.ID = fm.field0161
                  left join CTP_ENUM_ITEM as t8 on t8.ID = fm.field0282
                  join (select 商机编码 from tmp group by 商机编码) as tm on tm.商机编码 = fm.field0003)
select tmp2.*, tmp1.商机金额
from tmp2
         left join tmp1 on tmp2.商机编码 = tmp1.商机编码
--order by 创建日期 desc


--商密商机产品明细
SELECT fm.field0003                          as 商机编码,
       fm.field0004                          as 客户名称,
       t1.NAME                               as 销售人员,
       t2.NAME                               as 销售单位,
       fm.field0002                          as 商机名称,
       t3.SHOWVALUE                          as 项目状态,
       t4.SHOWVALUE                          as 销售阶段,
       t5.SHOWVALUE                          as 客户采购阶段,
       fm.field0017                          as 赢单率,
       fm.field0018                          as 预计成交时间, --预计成交时间
       fm.field0019                          as 预计成交季度, --预计成交季度
       t6.SHOWVALUE                          as 一级行业,     --一级行业
       t7.SHOWVALUE                          as 二级行业,     --二级行业
       t8.SHOWVALUE                          as 是否商密商机,
       fs.field0043                          as 产品线,
       fs.field0044                          as 产品型号,
       fs.field0082                          as 产品模块,
       fs.field0097                          as 料品编码,
       isnull(cei.SHOWVALUE, cei1.SHOWVALUE) as 产品是否商密,
       fs.field0045                          as 产品预计成交数量,
       fs.field0046                          as 产品预计成交金额,
       fs.field0084                          as 预计成交单价,
       fm.field0104                          as 创建日期,
       fm.field0103                          as 最后更新时间
FROM formmain_2156 fm
         left join formson_2157 fs on fs.formmain_id = fm.id
         left join formmain_1294 fm_12 on fs.field0097 = fm_12.field0008
         left join formmain_125708 fm_125 on fs.field0097 = fm_125.field0009
         left join CTP_ENUM_ITEM cei on cei.id = fm_12.field0165
         left join CTP_ENUM_ITEM cei1 on cei1.id = fm_125.field0033
         left join ORG_MEMBER as t1 on t1.id = fm.field0039
         left join ORG_UNIT as t2 on t2.ID = fm.field0040
         left join CTP_ENUM_ITEM as t3 on t3.ID = fm.field0028
         left join CTP_ENUM_ITEM as t4 on t4.ID = fm.field0016
         left join CTP_ENUM_ITEM as t5 on t5.id = fm.field0015
         left join CTP_ENUM_ITEM as t6 on t6.ID = fm.field0160
         left join CTP_ENUM_ITEM as t7 on t7.ID = fm.field0161
         left join CTP_ENUM_ITEM as t8 on t8.ID = fm.field0293
WHERE fm.field0286 is null
  and substring(fm.field0019, 1, 4) >= '2022'
  and fm.field0028 = '-4552238618822292475'        --项目状态‘进行中’
  AND fm.field0125 IS NULL
  AND fm.field0025 IS NOT NULL
  AND fm.field0003 is not null
  AND isnull(cei.SHOWVALUE, cei1.SHOWVALUE) = '是' --产品是否商密‘是’
  AND DATEDIFF(day, fm.field0018, '2007-01-01') <= 0
  AND DATEDIFF(day, fm.field0018, '2100-01-01') >= 0
--ORDER BY fm.field0104 desc



