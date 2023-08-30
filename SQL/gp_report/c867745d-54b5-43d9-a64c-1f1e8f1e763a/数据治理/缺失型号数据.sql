--产基缺失型号数据-验证使用
SELECT c."SHOWVALUE"          类型,
       d."NAME"               产线,
       field0145              产品名称,
       field0146              产品型号,
       field0147              产品模块,
       field0174              料品编码,
       case
           when field0147 IS NULL OR field0147 = '无' then field0146
           else field0147 end 最终产品型号
FROM ex_ods_oa_abv5_formson_1939 a
         LEFT JOIN ex_ods_oa_abv5_formmain_1119 b on a.formmain_id = b."ID"
         LEFT JOIN ex_ods_oa_abv5_CTP_ENUM_ITEM c ON a.field0144 = cast(c."ID" as text)
         LEFT JOIN ex_ods_oa_abv5_org_unit d ON a.field0509 = cast(d."ID" as text)
WHERE b.field0002 IS NOT NULL
  AND c."SHOWVALUE" <> '配件'
--   AND c."SHOWVALUE" <> '维保服务'
  AND field0429 IS NULL
  AND (field0545 IS NULL OR field0545 <> '-6437660009860739746')
  AND b.field0007 >= '2019-01-01'
  and (case
           when field0147 IS NULL OR field0147 = '无' then field0146
           else field0147 end) NOT IN
      (SELECT cpxh FROM ods_paas_uf_productmodel_infov2 where coabmzt = 0)
  AND (field0146 NOT IN
       (SELECT cpxh FROM ods_paas_uf_productmodel_infov2 where coabmzt = 0) OR
       field0146 IS NULL)
group by c."SHOWVALUE",
         d."NAME",
         field0145,
         field0146,
         field0147,
         field0174,
         case
             when a.field0147 IS NULL OR a.field0147 = '无' then a.field0146
             else a.field0147 end;


--产基已有型号数据
SELECT c."SHOWVALUE"             类型,
       field0145                 产品名称,
       pro.cpmc                  产基表名称,
       field0146                 产品型号,
       field0147                 产品模块,
       field0174                 料品编码,
       case
           when field0147 IS NULL OR field0147 = '无' then field0146
           else field0147 end    最终产品型号老,
       inf1.cpxh,
       inf2.cpxh,
       inf1.cpxhbm,
       inf2.cpxhbm,
       case
           when inf1.cpxh = inf2.cpxh then inf1.cpxh
           when inf1.cpxh is null then inf2.cpxh
           when inf2.cpxh is null then inf1.cpxh
           when inf1.cpxh is not null and inf2.cpxh is not null and inf1.cpxh <> inf2.cpxh
               then inf1.cpxh
           end                   最终产品型号,
       case
           when inf1.cpxhbm = inf2.cpxhbm then inf1.cpxhbm
           when inf1.cpxhbm is null then inf2.cpxhbm
           when inf2.cpxhbm is null then inf1.cpxhbm
           when inf1.cpxhbm is not null and inf2.cpxhbm is not null and
                inf1.cpxhbm <> inf2.cpxhbm then inf1.cpxhbm
           end                   最终产品型号编码,
       产品实际收入,
       max(b.field0007) as       评审时间,
       count(distinct field0002) 下单次数,
       sum(field0436)            合同金额
FROM ex_ods_oa_abv5_formson_1939 a
         LEFT JOIN ex_ods_oa_abv5_formmain_1119 b on a.formmain_id = b."ID"
         LEFT JOIN ex_ods_oa_abv5_CTP_ENUM_ITEM c ON a.field0144 = cast(c."ID" as text)
         LEFT JOIN ex_ods_oa_abv5_org_unit d ON a.field0509 = cast(d."ID" as text)
         left join (select *
                    from ods_paas_uf_productmodel_infov2
                    where coabmzt = 0) as inf1 on inf1.cpxh = a.field0146
         left join (select *
                    from ods_paas_uf_productmodel_infov2
                    where coabmzt = 0) as inf2 on inf2.cpxh = a.field0147
         left join (select field0004,
                           sum(field0278) 产品实际收入
                    from ex_ods_oa_abv5_formmain_105321 as a
                    GROUP BY field0004) as act on act.field0004 = a.field0174
         left join ods_paas_uf_product_infov2 as pro on pro.cpmc = field0145
-- WHERE b.field0002 IS NOT NULL
     -- AND c."SHOWVALUE" <> '配件'
-- --   AND c."SHOWVALUE" <> '维保服务'
--   AND field0429 IS NULL
--   AND (field0545 IS NULL OR field0545 <> '-6437660009860739746')
     --AND b.field0007 >= '2023-01-01'
group by c."SHOWVALUE",
         field0145,
         pro.cpmc,
         field0146,
         field0147,
         field0174,
         case
             when field0147 IS NULL OR field0147 = '无' then field0146
             else field0147 end,
         inf1.cpxh,
         inf2.cpxh,
         inf1.cpxhbm,
         inf2.cpxhbm,
         case
             when inf1.cpxh = inf2.cpxh then inf1.cpxh
             when inf1.cpxh is null then inf2.cpxh
             when inf2.cpxh is null then inf1.cpxh
             when inf1.cpxh is not null and inf2.cpxh is not null and
                  inf1.cpxh <> inf2.cpxh then inf1.cpxh
             end,
         case
             when inf1.cpxhbm = inf2.cpxhbm then inf1.cpxhbm
             when inf1.cpxhbm is null then inf2.cpxhbm
             when inf2.cpxhbm is null then inf1.cpxhbm
             when inf1.cpxhbm is not null and inf2.cpxhbm is not null and
                  inf1.cpxhbm <> inf2.cpxhbm then inf1.cpxhbm
             end,
         产品实际收入


---所有合同数据验证
select field0174,
       field0545,
       field0429,
       field0002,
       c."SHOWVALUE",
       field0007,
       case
           when field0147 IS NULL OR field0147 = '无' then field0146
           else field0147 end 最终产品型号老
FROM ex_ods_oa_abv5_formson_1939 a
         LEFT JOIN ex_ods_oa_abv5_formmain_1119 b on a.formmain_id = b."ID"
         LEFT JOIN ex_ods_oa_abv5_CTP_ENUM_ITEM c ON a.field0144 = cast(c."ID" as text)
         LEFT JOIN ex_ods_oa_abv5_org_unit d ON a.field0509 = cast(d."ID" as text)
         left join (select *
                    from ods_paas_uf_productmodel_infov2
                    where coabmzt = 0) as inf1 on inf1.cpxh = a.field0146
         left join (select *
                    from ods_paas_uf_productmodel_infov2
                    where coabmzt = 0) as inf2 on inf2.cpxh = a.field0147
where field0174 in ('CP020106-00055')




select a.field0008, a.field0107	涉密,a.field0017 渠道产品 ,b."SHOWVALUE" 是否渠道,c."SHOWVALUE" 是否涉密
from ex_ods_oa_abv5_formmain_1294 as a
LEFT JOIN ex_ods_oa_abv5_CTP_ENUM_ITEM b ON a.field0017 = b."ID"
LEFT JOIN ex_ods_oa_abv5_CTP_ENUM_ITEM c ON a.field0107 =cast(c."ID" as text)
where a.field0008 is not null
group by field0008, field0107,field0017,b."SHOWVALUE",c."SHOWVALUE"


--产品数据
select m.cpxbm,                               --产品线编码
       cpx.cpx                 pro_line_name,--产线名称,
       m.cpflbm,                              --产品分类编码
       cpclass.cpfl            pro_class_name,--产品分类名称
       m.cpbm,                                --产品编码
       cp.cpmc                 pro_name,--产品名称
       cpxhbm,                                --产品型号编码
       cpxh,                                  --产品型号
       coabm,                                 --COA编码
       case
           when coabmzt = 1 then '失效'
           when coabmzt = 0
               then '生效' end coa_status     --COA编码状态
from ods_paas_uf_productmodel_infov2 as m
         left join ex_ods_pass_ecology_hrmresource as h1 on m.cpjl = h1.id
         left join ex_ods_pass_ecology_hrmresource as h2 on m.cpzj = cast(h2.id as text)
         left join ods_paas_uf_productline_infov2 as cpx on cpx.cpxbm = m.cpxbm
         left join ods_paas_uf_productclass_infov2 as cpclass on cpclass.cpflbm = m.cpflbm
         left join ods_paas_uf_product_infov2 as cp on cp.cpbm = m.cpbm



update formmain_1294
set field0198='0310015'where field0008='CP010101-00024';

update formson_1939
set field1201 ='0310015'where field0174='CP010101-00024';



SELECT *
FROM ods_oa_formmain_2156
limit 10