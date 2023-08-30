with tmp  as (select f1.id,
     f1.field0007 as 审批完成时间,
     f1.field0002 as 合同编号,
     f1.field0003 as 合同单位,
     f1.field0004 as 最终客户名称,
     f1.field0005 as 销售人员,
     f1.field0006 as 归属部门,
     f1.field0380 as 客户一级行业,
     f1.field0381 as 客户二级行业,
     f1.field0255 as 合同共享人,
    (select SHOWVALUE from CTP_ENUM_ITEM where id=f1.field0853) 是否信创合同,
    (select SHOWVALUE from CTP_ENUM_ITEM where id=f1.field0250) 销售方式,
     cast(f1.field0011/10000 as decimal(10,3)) as 合同总金额,
     cast(f1.field0013  as decimal(10,3)) as 已回款比例
     FROM formmain_1119 f1
     where
     f1.field0002 in (select distinct f1.field0002 FROM formmain_1119 f1
     left join formson_1939 f2 on f1.id=f2.formmain_id
     left join formmain_1294 f3 on f2.field0174=f3.field0008
     where
     (f1.field0853 ='-7086773742814412536' or f3.field0037='是')--信创合同或信创产品
     and f1.field0006<>'北京安恒网安营销中心'
     and (select SHOWVALUE from CTP_ENUM_ITEM where id=f1.field0250)<>'内部结算'
     ${if(len(月)=0,"","and month(f1.field0007) in ('"+月+"')")}
     ${if(len(年)=0,"","and year(f1.field0007) in ('"+年+"')")})
     ),
     tmp1 as (
     select formmain_id,cast(sum(f2.field0436/10000) as decimal(10,3)) as 产品总金额 from formson_1939 f2
     left join formmain_1119 f1 on f1.id=f2.formmain_id
     left join formmain_1294 f3 on f2.field0174=f3.field0008
     where
     f3.field0037='是' --信创产品
     and f1.field0006<>'北京安恒网安营销中心'
     and (select SHOWVALUE from CTP_ENUM_ITEM where id=f1.field0250)<>'内部结算'
     ${if(len(月)=0,"","and month(f1.field0007) in ('"+月+"')")}
     ${if(len(年)=0,"","and year(f1.field0007) in ('"+年+"')")}
     group by formmain_id
     )
     select tmp.*,tmp1.产品总金额 from tmp left join tmp1 on tmp.id=tmp1.formmain_id
     where 合同总金额<>0
     --and 产品总金额<>0
     order by tmp.审批完成时间 desc




with tmp  as (select f1.id,
     f1.field0007 as 审批完成时间,
     f1.field0002 as 合同编号,
     f1.field0003 as 合同单位,
     f1.field0004 as 最终客户名称,
     f1.field0005 as 销售人员,
     f1.field0006 as 归属部门,
     f1.field0380 as 客户一级行业,
     f1.field0381 as 客户二级行业,
     f1.field0255 as 合同共享人,
    (select SHOWVALUE from CTP_ENUM_ITEM where id=f1.field0853) 是否信创合同,
    (select SHOWVALUE from CTP_ENUM_ITEM where id=f1.field0250) 销售方式,
     cast(f1.field0011/10000 as decimal(10,3)) as 合同总金额,
     cast(f1.field0013  as decimal(10,3)) as 已回款比例
     FROM formmain_1119 f1
     where
     f1.field0002 in (select distinct f1.field0002 FROM formmain_1119 f1
     left join formson_1939 f2 on f1.id=f2.formmain_id
     left join formmain_1294 f3 on f2.field0174=f3.field0008
     where
     (f1.field0853 ='-7086773742814412536' or f3.field0037='是')--信创合同或信创产品
     and f1.field0006<>'北京安恒网安营销中心'
     and (select SHOWVALUE from CTP_ENUM_ITEM where id=f1.field0250)<>'内部结算'
     ${if(len(月)=0,"","and month(f1.field0007) in ('"+月+"')")}
     ${if(len(年)=0,"","and year(f1.field0007) in ('"+年+"')")})
     ),
     tmp1 as (
     select formmain_id,cast(sum(f2.field0436/10000) as decimal(10,3)) as 产品总金额 from formson_1939 f2
     left join formmain_1119 f1 on f1.id=f2.formmain_id
     left join formmain_1294 f3 on f2.field0174=f3.field0008
     where
     f3.field0037='是' --信创产品
     and f1.field0006<>'北京安恒网安营销中心'
     and (select SHOWVALUE from CTP_ENUM_ITEM where id=f1.field0250)<>'内部结算'
     ${if(len(月)=0,"","and month(f1.field0007) in ('"+月+"')")}
     ${if(len(年)=0,"","and year(f1.field0007) in ('"+年+"')")}
     group by formmain_id
     )
     select tmp.*,tmp1.产品总金额 from tmp left join tmp1 on tmp.id=tmp1.formmain_id
     where 合同总金额<>0
     --and 产品总金额<>0
     order by tmp.审批完成时间 desc



select f1.id,
       f1.field0007                                 as               审批完成时间,
       f1.field0002                                 as               合同编号,
       f1.field0003                                 as               合同单位,
       f1.field0004                                 as               最终客户名称,
       f1.field0005                                 as               销售人员,
       f1.field0006                                 as               归属部门,
       f1.field0380                                 as               客户一级行业,
       f1.field0381                                 as               客户二级行业,
       f1.field0255                                 as               合同共享人,
       t1.SHOWVALUE 是否信创合同,
       t2.SHOWVALUE 销售方式,
       cast(f1.field0011 / 10000 as decimal(10, 3)) as               合同总金额,
       cast(f1.field0013 as decimal(10, 3))         as               已回款比例
FROM formmain_1119 as f1
         left join formson_1939 f2 on f1.id = f2.formmain_id
         left join formmain_1294 f3 on f2.field0174 = f3.field0008
         left join CTP_ENUM_ITEM as t1 on t1.id = f1.field0853
         left join CTP_ENUM_ITEM as t2 on t2.id = f1.field0250
