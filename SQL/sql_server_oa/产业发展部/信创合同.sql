--信创合同明细
with tmp as (select f1.id,
                    f1.field0007                                 as 审批完成时间,
                    f1.field0002                                 as 合同编号,
                    f1.field0003                                 as 合同单位,
                    f1.field0004                                 as 最终客户名称,
                    f1.field0005                                 as 销售人员,
                    f1.field0006                                 as 归属部门,
                    f1.field0380                                 as 客户一级行业,
                    f1.field0381                                 as 客户二级行业,
                    f1.field0255                                 as 合同共享人,
                    t1.SHOWVALUE                                    是否信创合同,
                    t2.SHOWVALUE                                    销售方式,
                    cast(f1.field0011 / 10000 as decimal(10, 3)) as 合同总金额,
                    f1.field0577                                    合同折扣,
                    cast(f1.field0013 as decimal(10, 3))         as 已回款比例
             FROM formmain_1119 f1
                      left join CTP_ENUM_ITEM as t1 on t1.id = f1.field0853
                      left join CTP_ENUM_ITEM as t2 on t2.id = f1.field0250
                      join (select distinct f1.field0002
                            FROM formmain_1119 f1
                                     left join formson_1939 f2 on f1.id = f2.formmain_id
                                     left join formmain_1294 f3 on f2.field0174 = f3.field0008
                            join  CTP_ENUM_ITEM as t3 on t3.id = f1.field0250
                            where (f1.field0853 = '-7086773742814412536' or f3.field0037 = '是')--信创合同或信创产品
                              and f1.field0006 <> '北京安恒网安营销中心'
                              and t3.SHOWVALUE <> '内部结算') as t3
                           on t3.field0002 = f1.field0002),
     tmp1 as (select formmain_id, cast(sum(f2.field0436 / 10000) as decimal(10, 3)) as 产品总金额
              from formson_1939 f2
                       left join formmain_1119 f1 on f1.id = f2.formmain_id
                       left join formmain_1294 f3 on f2.field0174 = f3.field0008
                       join CTP_ENUM_ITEM as t3 on t3.id = f1.field0250
              where f3.field0037 = '是' --信创产品
                and f1.field0006 <> '北京安恒网安营销中心'
                and t3.SHOWVALUE <> '内部结算'
              group by formmain_id)
select tmp.*, tmp1.产品总金额
from tmp
         left join tmp1 on tmp.id = tmp1.formmain_id
where 合同总金额 <> 0


--商密合同明细
with tmp as (select f1.id,
                    f1.field0007                                 as 审批完成时间,
                    f1.field0002                                 as 合同编号,
                    f1.field0003                                 as 合同单位,
                    f1.field0004                                 as 最终客户名称,
                    f1.field0005                                 as 销售人员,
                    f1.field0006                                 as 归属部门,
                    f1.field0380                                 as 客户一级行业,
                    f1.field0381                                 as 客户二级行业,
                    f1.field0255                                 as 合同共享人,
                    t1.SHOWVALUE                                    是否信创合同,
                    t2.SHOWVALUE                                    销售方式,
                    cast(f1.field0011 / 10000 as decimal(10, 3)) as 合同总金额,
                    field0487 总进销差价,
                    f1.field0577                                    合同折扣,
                    cast(f1.field0013 as decimal(10, 3))         as 已回款比例
             FROM formmain_1119 f1
                      left join CTP_ENUM_ITEM as t1 on t1.id = f1.field0853
                      left join CTP_ENUM_ITEM as t2 on t2.id = f1.field0250
                      join (select distinct f1.field0002
                            FROM formmain_1119 f1
                                     left join formson_1939 f2 on f1.id = f2.formmain_id
                                     left join formmain_1294 f3 on f2.field0174 = f3.field0008
                                     join CTP_ENUM_ITEM as t3 on t3.id = f1.field0250
                                     join CTP_ENUM_ITEM as t4 on t4.id = f3.field0165
                            where t4.SHOWVALUE = '是' ----是否商密产品
                              and f1.field0006 <> '北京安恒网安营销中心'
                              and t3.SHOWVALUE <> '内部结算') as t3
                           on t3.field0002 = f1.field0002),
     tmp1 as (select formmain_id, cast(sum(f2.field0436 / 10000) as decimal(10, 3)) as 产品总金额
              from formson_1939 f2
                       left join formmain_1119 f1 on f1.id = f2.formmain_id
                       left join formmain_1294 f3 on f2.field0174 = f3.field0008
                       join CTP_ENUM_ITEM as t3 on t3.id = f1.field0250
                       join CTP_ENUM_ITEM as t4 on t4.id = f3.field0165
              where t4.SHOWVALUE = '是' ----是否商密产品
                and f1.field0006 <> '北京安恒网安营销中心'
                and t3.SHOWVALUE <> '内部结算'
              group by formmain_id)
select tmp.*, tmp1.产品总金额
from tmp
         left join tmp1 on tmp.id = tmp1.formmain_id
where 合同总金额 <> 0


--子公司合同明细
with tmp as (select f1.id,
                    f1.field0007                                 as 审批完成时间,
                    f1.field0002                                 as 合同编号,
                    f1.field0003                                 as 合同单位,
                    f1.field0004                                 as 最终客户名称,
                    f1.field0005                                 as 销售人员,
                    f1.field0006                                 as 归属部门,
                    f1.field0380                                 as 客户一级行业,
                    f1.field0381                                 as 客户二级行业,
                    f1.field0255                                 as 合同共享人,
                    t1.SHOWVALUE                                    是否信创合同,
                    t2.SHOWVALUE                                    销售方式,
                    cast(f1.field0011 / 10000 as decimal(10, 3)) as 合同总金额,
                    f1.field0577                                    合同折扣,
                    cast(f1.field0013 as decimal(10, 3))         as 已回款比例
             FROM formmain_1119 f1
                      left join CTP_ENUM_ITEM as t1 on t1.id = f1.field0853
                      left join CTP_ENUM_ITEM as t2 on t2.id = f1.field0250
                      join (select distinct f1.field0002
                            FROM formmain_1119 f1
                                     left join formson_1939 f2 on f1.id = f2.formmain_id
                                     left join formmain_1294 f3 on f2.field0174 = f3.field0008
                                     join CTP_ENUM_ITEM as t3 on t3.id = f1.field0250
                                     join CTP_ENUM_ITEM as t4 on t4.id = f3.field0165
                            where ((f1.field0853 = '-7086773742814412536' or f3.field0037 = '是') or
                                   t4.SHOWVALUE = '是')
                             -- and f1.field0006 <> '北京安恒网安营销中心'
                              and t3.SHOWVALUE = '内部结算') as t3
                           on t3.field0002 = f1.field0002),
     tmp1 as (select formmain_id, cast(sum(f2.field0436 / 10000) as decimal(10, 3)) as 产品总金额
              from formson_1939 f2
                       left join formmain_1119 f1 on f1.id = f2.formmain_id
                       left join formmain_1294 f3 on f2.field0174 = f3.field0008
                       join CTP_ENUM_ITEM as t3 on t3.id = f1.field0250
                       join CTP_ENUM_ITEM as t4 on t4.id = f3.field0165
              where ((f1.field0853 = '-7086773742814412536' or f3.field0037 = '是') or t4.SHOWVALUE = '是')
                --and f1.field0006 <> '北京安恒网安营销中心'
                and t3.SHOWVALUE = '内部结算'
              group by formmain_id)
select tmp.*, tmp1.产品总金额
from tmp
         left join tmp1 on tmp.id = tmp1.formmain_id
where 合同总金额 <> 0


--信创合同产品明细
with tmp as (select f1.id,
                    f1.field0007                                 as 审批完成时间,
                    f1.field0002                                 as 合同编号,
                    f1.field0003                                 as 合同单位,
                    f1.field0004                                 as 最终客户名称,
                    f1.field0005                                 as 销售人员,
                    f1.field0006                                 as 归属部门,
                    f1.field0380                                 as 客户一级行业,
                    f1.field0381                                 as 客户二级行业,
                    f1.field0255                                 as 合同共享人,
                    t1.SHOWVALUE                                    产品是否商密,
                    f3.field0037                                    产品是否信创,
                    t2.SHOWVALUE                                    是否信创,
                    t3.SHOWVALUE                                    销售方式,
                    t4.SHOWVALUE                                 as 产品类型,
                    f2.field0145                                 as 产品名称,
                    f2.field0146                                 as 产品型号,
                    f2.field0147                                 as 产品模块,
                    f2.field0174                                 as 料品编码,
                    f2.field0426                                 as 产品数量,
                    case
                        when (select SHOWVALUE from CTP_ENUM_ITEM cei where id = f2.field0534) = '是' then f2.field0518
                        else f2.field0315 end                    as 产品单价,
                    f2.field0436                                 as 合同产品金额,
                    f2.field0519                                    产品折扣,
                    cast(f1.field0011 / 10000 as decimal(10, 3)) as 合同总金额,
                    f2.field0517 as 内部核算金额,
                    f1.field0013                                 as 已回款比例
             FROM formmain_1119 f1
                      left join formson_1939 f2 on f1.id = f2.formmain_id
                      left join formmain_1294 f3 on f2.field0174 = f3.field0008
                      left join CTP_ENUM_ITEM as t1 on t1.id = f2.field1025
                      left join CTP_ENUM_ITEM as t2 on t2.id = f1.field0853
                      left join CTP_ENUM_ITEM as t3 on t3.id = f1.field0250
                      left join CTP_ENUM_ITEM as t4 on t4.id = f2.field0144
                      join CTP_ENUM_ITEM as t5 on t5.id = f1.field0250
             where (f1.field0853 = '-7086773742814412536' or f3.field0037 = '是')--信创合同或信创产品
               and f1.field0006 <> '北京安恒网安营销中心'
               and t5.SHOWVALUE <> '内部结算'),
     tmp1 as (select 合同编号, sum(合同产品金额) as 产品总金额
              from tmp
              where 产品是否信创 = '是'
              group by 合同编号)
select tmp.*, tmp1.产品总金额
from tmp
         left join tmp1 on tmp.合同编号 = tmp1.合同编号
where 合同总金额 <> 0
  and 合同产品金额 <> 0




--商密合同产品明细
with tmp as (select f1.id,
                    f1.field0007                                 as 审批完成时间,
                    f1.field0002                                 as 合同编号,
                    f1.field0003                                 as 合同单位,
                    f1.field0004                                 as 最终客户名称,
                    f1.field0005                                 as 销售人员,
                    f1.field0006                                 as 归属部门,
                    f1.field0380                                 as 客户一级行业,
                    f1.field0381                                 as 客户二级行业,
                    f1.field0255                                 as 合同共享人,
                    t1.SHOWVALUE                                    产品是否商密,
                    f3.field0037                                    产品是否信创,
                    t2.SHOWVALUE                                    是否信创,
                    t3.SHOWVALUE                                    销售方式,
                    t4.SHOWVALUE                                 as 产品类型,
                    f2.field0145                                 as 产品名称,
                    f2.field0146                                 as 产品型号,
                    f2.field0147                                 as 产品模块,
                    f2.field0174                                 as 料品编码,
                    f2.field0426                                 as 产品数量,
                    case
                        when (select SHOWVALUE from CTP_ENUM_ITEM cei where id = f2.field0534) = '是' then f2.field0518
                        else f2.field0315 end                    as 产品单价,
                    f2.field0436                                 as 合同产品金额,
                    f2.field0519                                    产品折扣,
                    cast(f1.field0011 / 10000 as decimal(10, 3)) as 合同总金额,
                    f1.field0013                                 as 已回款比例
             FROM formmain_1119 f1
                      left join formson_1939 f2 on f1.id = f2.formmain_id
                      left join formmain_1294 f3 on f2.field0174 = f3.field0008
                      left join CTP_ENUM_ITEM as t1 on t1.id = f2.field1025
                      left join CTP_ENUM_ITEM as t2 on t2.id = f1.field0853
                      left join CTP_ENUM_ITEM as t3 on t3.id = f1.field0250
                      left join CTP_ENUM_ITEM as t4 on t4.id = f2.field0144
                      join CTP_ENUM_ITEM as t5 on t5.id = f1.field0250
                    join  CTP_ENUM_ITEM as t6 on t6.id = f3.field0165
             where t6.SHOWVALUE = '是' ----是否商密产品
               and f1.field0006 <> '北京安恒网安营销中心'
               and t5.SHOWVALUE <> '内部结算'),
     tmp1 as (select 合同编号, sum(合同产品金额) as 产品总金额
              from tmp
              where 产品是否信创 = '是'
              group by 合同编号)
select tmp.*, tmp1.产品总金额
from tmp
         left join tmp1 on tmp.合同编号 = tmp1.合同编号
where 合同总金额 <> 0
  and 合同产品金额 <> 0


--子公司合同产品明细
with tmp as (select f1.id,
                    f1.field0007                                 as 审批完成时间,
                    f1.field0002                                 as 合同编号,
                    f1.field0003                                 as 合同单位,
                    f1.field0004                                 as 最终客户名称,
                    f1.field0005                                 as 销售人员,
                    f1.field0006                                 as 归属部门,
                    f1.field0380                                 as 客户一级行业,
                    f1.field0381                                 as 客户二级行业,
                    f1.field0255                                 as 合同共享人,
                    t1.SHOWVALUE                                    产品是否商密,
                    f3.field0037                                    产品是否信创,
                    t2.SHOWVALUE                                    是否信创,
                    t3.SHOWVALUE                                    销售方式,
                    t4.SHOWVALUE                                 as 产品类型,
                    f2.field0145                                 as 产品名称,
                    f2.field0146                                 as 产品型号,
                    f2.field0147                                 as 产品模块,
                    f2.field0174                                 as 料品编码,
                    f2.field0426                                 as 产品数量,
                    case
                        when (select SHOWVALUE from CTP_ENUM_ITEM cei where id = f2.field0534) = '是' then f2.field0518
                        else f2.field0315 end                    as 产品单价,
                    f2.field0436                                 as 合同产品金额,
                    f2.field0519                                    产品折扣,
                    cast(f1.field0011 / 10000 as decimal(10, 3)) as 合同总金额,
                    f1.field0013                                 as 已回款比例
             FROM formmain_1119 f1
                      left join formson_1939 f2 on f1.id = f2.formmain_id
                      left join formmain_1294 f3 on f2.field0174 = f3.field0008
                      left join CTP_ENUM_ITEM as t1 on t1.id = f2.field1025
                      left join CTP_ENUM_ITEM as t2 on t2.id = f1.field0853
                      left join CTP_ENUM_ITEM as t3 on t3.id = f1.field0250
                      left join CTP_ENUM_ITEM as t4 on t4.id = f2.field0144
                      join CTP_ENUM_ITEM as t5 on t5.id = f1.field0250
                      join CTP_ENUM_ITEM as t6 on t6.id = f3.field0165
             where ((f1.field0853 = '-7086773742814412536' or f3.field0037 = '是') or t6.SHOWVALUE = '是')
               -- and f1.field0006 <> '北京安恒网安营销中心'
               and t5.SHOWVALUE = '内部结算'),
     tmp1 as (select 合同编号, sum(合同产品金额) as 产品总金额
              from tmp
              where 产品是否信创 = '是'
              group by 合同编号)
select tmp.*, tmp1.产品总金额
from tmp
         left join tmp1 on tmp.合同编号 = tmp1.合同编号
where 合同总金额 <> 0
  and 合同产品金额 <> 0
