--信创商机明细
SELECT fm.field0003      商机编码,
       fm.field0004      客户名称,
       t1.NAME           销售人员,
       t2.NAME           销售单位,
       fm.field0002      商机名称,
       (case fm.field0121
            when -1582778678960566746 then fm.field0022
            when -1509712256699316103 then fm.field0022
            else '' end) 渠道,
       t3.SHOWVALUE      项目状态,
       t4.SHOWVALUE      销售阶段,
       t5.SHOWVALUE      客户采购阶段,
       fm.field0017      赢单率,
       t6.SHOWVALUE      预测,
       fm.field0018      预计成交时间,
       fm.field0019      预计成交季度,
       fm.field0025      商机金额,
       tt1.SHOWVALUE     已计入FC,
       tt2.SHOWVALUE     是否创新项目,
       tt3.SHOWVALUE     是否涉密项目,
       tt4.SHOWVALUE     一级行业,
       tt5.SHOWVALUE     二级行业,
       tt6.SHOWVALUE     公安科信项目类型,
       fm.field0104      创建日期,
       fm.field0103      最后更新时间,
       fm.field0024      备注,
       fm.field0171,
       fm.field0030,
       fm.field0031,
       fm.field0032,
       fm.field0033,
       fm.field0034,
       fm.field0157,
       fm.field0158,
       fm.field0039 as   field0039id,
       fm.field0040,
       fm.ID,
       fm.field0005      客户编码,
       t7.SHOWVALUE as   是否商密,
       t8.SHOWVALUE as   三级行业,
       t9.SHOWVALUE as   CRM销售阶段
FROM formmain_2156 fm
         left join ORG_MEMBER as t1 on t1.id = fm.field0039
         left join ORG_UNIT as t2 on t2.ID = fm.field0040
         left join CTP_ENUM_ITEM as t3 on t3.ID = fm.field0028
         left join CTP_ENUM_ITEM as t4 on t4.ID = fm.field0016
         left join CTP_ENUM_ITEM as t5 on t5.id = fm.field0015
         left join CTP_ENUM_ITEM as t6 on t6.ID = fm.field0014
         left join CTP_ENUM_ITEM as tt1 on tt1.ID = fm.field0037
         left join CTP_ENUM_ITEM as tt2 on tt2.ID = fm.field0189
         left join CTP_ENUM_ITEM as tt3 on tt3.ID = fm.field0192
         left join CTP_ENUM_ITEM as tt4 on tt4.ID = fm.field0160
         left join CTP_ENUM_ITEM as tt5 on tt5.ID = fm.field0161
         left join CTP_ENUM_ITEM as tt6 on tt6.ID = fm.field0188
         left join CTP_ENUM_ITEM as t7 on t7.ID = field0293
         left join CTP_ENUM_ITEM as t8 on t8.ID = field0282
         left join CTP_ENUM_ITEM as t9 on t9.ID = field0298
WHERE field0286 is null
  and left(field0019, 4) >= 2022
  and field0028 = '-4552238618822292475' --项目状态‘进行中’
  AND field0125 IS NULL
  AND field0025 IS NOT NULL
  AND field0003 is not null
  AND field0189 = '-7086773742814412536' --是否信创‘是’
  AND DATEDIFF(day, field0018, '2007-01-01') <= 0
  AND DATEDIFF(day, field0018, '2100-01-01') >= 0


--信创商机产品明细
SELECT fm.field0003    as 商机编码,
       fm.field0004    as 客户名称,
       t1.NAME         as 销售人员,
       t2.NAME         as 销售单位,
       fm.field0002    as 商机名称,
       t3.SHOWVALUE    as 项目状态,
       t4.SHOWVALUE    as 销售阶段,
       t5.SHOWVALUE    as 客户采购阶段,
       fm.field0017    as 赢单率,
       t6.SHOWVALUE    as 预测,         --预测
       fm.field0018    as 预计成交时间, --预计成交时间
       fm.field0019    as 预计成交季度, --预计成交季度
       fm.field0025    as 商机金额,     --商机金额
       t7.SHOWVALUE    as 一级行业,     --一级行业
       t8.SHOWVALUE    as 二级行业,     --二级行业
       t9.SHOWVALUE    as 是否信创商机,
       fs.field0043    as 产品线,
       fs.field0044    as 产品型号,
       fs.field0082    as 产品模块,
       fs.field0097    as 料品编码,
       fm_12.field0037 as 是否信创产品,
       fs.field0045    as 产品预计成交数量,
       fs.field0046    as 产品预计成交金额,
       fs.field0084    as 预计成交单价,
       fm.field0104    as 创建日期,
       fm.field0103    as 最后更新时间
FROM formmain_2156 fm
         left join formson_2157 fs on fs.formmain_id = fm.id
         left join formmain_1294 fm_12 on fs.field0097 = fm_12.field0008
         left join ORG_MEMBER as t1 on t1.id = fm.field0039
         left join ORG_UNIT as t2 on t2.ID = fm.field0040
         left join CTP_ENUM_ITEM as t3 on t3.ID = fm.field0028
         left join CTP_ENUM_ITEM as t4 on t4.ID = fm.field0016
         left join CTP_ENUM_ITEM as t5 on t5.id = fm.field0015
         left join CTP_ENUM_ITEM as t6 on t6.ID = fm.field0014
         left join CTP_ENUM_ITEM as t7 on t7.ID = fm.field0160
         left join CTP_ENUM_ITEM as t8 on t8.ID = fm.field0161
         left join CTP_ENUM_ITEM as t9 on t9.ID = fm.field0189
WHERE fm.field0286 is null
  and left(fm.field0019, 4) >= 2022
  and fm.field0028 = '-4552238618822292475' --项目状态‘进行中’
  AND fm.field0125 IS NULL
  AND fm.field0025 IS NOT NULL
  AND fm.field0003 is not null
  AND fm.field0189 = '-7086773742814412536' --是否信创‘是’
  AND DATEDIFF(day, fm.field0018, '2007-01-01') <= 0
  AND DATEDIFF(day, fm.field0018, '2100-01-01') >= 0
