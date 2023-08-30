SELECT c.SHOWVALUE                                                      类型,
       field0509                                                        产品所属部门,
       d.NAME 产线,
       field0145                                                        产品名称,
       field0146                                                        产品型号,
       field0147                                                        产品模块,
       field0174                                                        料品编码,
       IIF(field0147 IS NULL OR field0147 = '无', field0146, field0147) 最终产品型号
FROM formson_1939 a
         LEFT JOIN formmain_1119 b on a.formmain_id = b.ID
         LEFT JOIN dbo.CTP_ENUM_ITEM c ON a.field0144 = c.ID
         LEFT JOIN dbo.org_unit D ON a.field0509 = D.ID
        left join formmain_125708 as t on t.field0010 = IIF(field0147 IS NULL OR field0147 = '无', field0146, field0147)
WHERE --field0145 IS NOT NULL
    b.field0002 IS NOT NULL
  AND c.SHOWVALUE <> '服务'
  AND c.SHOWVALUE <> '维保服务'
  --AND IIF(field0147 IS NULL OR field0147 = '无', field0146, field0147) NOT IN (SELECT field0010 FROM formmain_125708)
  AND field0429 IS NULL
  AND (field0545 IS NULL OR field0545 <> -6437660009860739746)
  --AND (field0146 NOT IN (SELECT field0010 FROM formmain_125708) OR field0146 IS NULL)
  AND b.field0007 >= '2019-01-01'
  and  t.field0010 is null
group by c.SHOWVALUE,
         field0509,
         d.NAME,
         field0145,
         field0146,
         field0147,
         field0174,
         IIF(field0147 IS NULL OR field0147 = '无', field0146, field0147)

select count(DISTINCT field0010) FROM formmain_125708 where field0012='生效'

select * from formmain_125708