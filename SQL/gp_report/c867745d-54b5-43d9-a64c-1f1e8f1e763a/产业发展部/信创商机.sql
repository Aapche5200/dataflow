SELECT
field0003, --商机编码
field0004, --客户名称
(select NAME from ORG_MEMBER where id=field0039) as field0039,--销售人员
(select NAME from ORG_UNIT where ID=formmain_2156.field0040) xiaoshoudanwei,--销售单位
field0002, --商机名称
(case field0121 when -1582778678960566746 then field0022 when -1509712256699316103 then field0022 else '' end) qudao, --渠道
(select SHOWVALUE from CTP_ENUM_ITEM where ID=field0028) as field0028, --项目状态
(select SHOWVALUE from CTP_ENUM_ITEM where ID=field0016) as field0016, --销售阶段
(select SHOWVALUE from CTP_ENUM_ITEM where id=field0015) as field0015, --客户采购阶段
field0017, --赢单率
(select SHOWVALUE from CTP_ENUM_ITEM where ID=field0014) as field0014, --预测
field0018, --预计成交时间
field0019, --预计成交季度
field0025 商机金额, --商机金额
(select SHOWVALUE from CTP_ENUM_ITEM where ID=field0037) as field0037, --已计入FC
(select SHOWVALUE from CTP_ENUM_ITEM where ID=field0189) as field0189, --是否创新项目
(select SHOWVALUE from CTP_ENUM_ITEM where ID=field0192) as field0192, --是否涉密项目
(select SHOWVALUE from CTP_ENUM_ITEM where ID=field0160) as field0008, --一级行业
(select SHOWVALUE from CTP_ENUM_ITEM where ID=field0161) as field0009, --二级行业
(select SHOWVALUE from CTP_ENUM_ITEM where ID=field0188) as field0188, --公安科信项目类型
field0104, --创建日期
field0103, --最后更新时间
field0024, --备注
field0171,
field0030,
field0031,
field0032,
field0033,
field0034,
field0157,
field0158,
field0039 as field0039id,
field0040,
ID,
field0015, -- 客户采购阶段
field0005, -- 客户编码
(select SHOWVALUE from CTP_ENUM_ITEM where ID=field0293) as field0293, --是否商密
(select SHOWVALUE from CTP_ENUM_ITEM where ID=field0282) as field0282, --三级行业
(select SHOWVALUE from CTP_ENUM_ITEM where ID=field0298) as field0298  --CRM销售阶段
FROM formmain_2156
--` + businessIdsStr + `
WHERE field0286 is null
and left(field0019,4)>=2022
and field0028='-4552238618822292475' --项目状态‘进行中’
AND field0125 IS NULL  AND field0025 IS NOT NULL
AND field0003 is not null AND field0189='-7086773742814412536' --是否信创‘是’
--` + sql_filter + `
AND DATEDIFF(day,field0018,'2007-01-01')<=0 AND DATEDIFF(day,field0018,'2100-01-01')>=0
ORDER BY field0104 desc
