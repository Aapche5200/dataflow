--paas产品型号信息表
select * from dwd_paas_uf_productmodel_infov2 limit 0 --确定计量单位是否需要保留
truncate table dwd_paas_uf_productmodel_infov2;
insert into dwd_paas_uf_productmodel_infov2
select date(coabmsxrq)                                                           coabmsxrq,     --COA编码失效日期
       m.cpzj,                                                                                    --产品总监
       h2.lastname                                                               director_name,--产品总监姓名
       case
           when m.cpxt = '0' then '产品'
           when m.cpxt = '1' then '服务'
           when m.cpxt = '2' then '产品+服务'
           end                                                                   pro_xt,--产品形态
       m.cpxbm,                                                                                 --产品线编码
       cpx.cpx                                                                   pro_line_name,--产线名称,
       m.cpflbm,                                                                                --产品分类编码
       cpclass.cpfl                                                              pro_class_name,--产品分类名称
       m.cpbm,                                                                                  --产品编码
       cp.cpmc                                                                    pro_name,--产品名称
       cpmcxsmc,                                                                                --产品销售名称
       cpxhbm,                                                                                  --产品型号编码
       cpxh,                                                                                    --产品型号
       coabm,                                                                                   --COA编码
       case when coabmzt = 1 then '失效' when coabmzt = 0 then '生效' end        coa_status,    --COA编码状态
       m.cpjl,                                                                                    --型号产品经理
       h1.lastname                                                            as manager_name,--产品经理姓名
       to_char(to_timestamp(cast(m.simpledate as int8) / 1000), 'YYYY-MM-DD') as event_date,--更新时间
       t1."SHOWVALUE"                                                         as cplx,          --产品类型
       gnms,                                                                                    --功能描述
       t2."SHOWVALUE"                                                         as cpsmzqzt,      --产品生命周期状态
       (tzxseom)                                                                 tzxseom,       --停止销售EOM日期
       (tzqmzceofs)                                                              tzqmzceofs,    --停止全面支持EOFS日期
       (tzfweos)                                                                 tzfweos,       --停止服务EOS日期
       xksm,                                                                                    --许可说明
       pzzd,                                                                                    --配置指导
       t3."SHOWVALUE"                                                         as xyqdzx,        --行业渠道属性
       shyhlx,                                                                                  --适合用户类型
       jldw,                                                                                    --计量单位
       t4."SHOWVALUE"                                                            sfsm,          --是否涉密
       t5."SHOWVALUE"                                                            sfxc,          --是否信创
       t6."SHOWVALUE"                                                            sfshangm,      --是否商密
       xxxhbb,                                                                                  --销许型号/版本
       t7."SHOWVALUE"                                                         as xhlb,          --型号类别
       ggpz,                                                                                    --规格配置
       kkzsm,                                                                                   --可扩展说明
       xn,                                                                                      --性能
       jldwzwmc                                                                                 --计量单位中文名称
from ods_paas_uf_productmodel_infov2 as m
         left join ex_ods_pass_ecology_hrmresource as h1 on m.cpjl = h1.id
         left join ex_ods_pass_ecology_hrmresource as h2 on m.cpzj = cast(h2.id as text)
         left join ex_ods_oa_abv5_ctp_enum_item t1 on cast(m.cplx as text) = cast(t1."ID" as text)
         left join ex_ods_oa_abv5_ctp_enum_item t2 on cast(m.cpsmzqzt as text) = cast(t2."ID" as text)
         left join ex_ods_oa_abv5_ctp_enum_item t3 on cast(m.xyqdzx as text) = cast(t3."ID" as text)
         left join ex_ods_oa_abv5_ctp_enum_item t4 on cast(m.sfsm as text) = cast(t4."ID" as text)
         left join ex_ods_oa_abv5_ctp_enum_item t5 on cast(m.sfxc as text) = cast(t5."ID" as text)
         left join ex_ods_oa_abv5_ctp_enum_item t6 on cast(m.sfshangm as text) = cast(t6."ID" as text)
         left join ex_ods_oa_abv5_ctp_enum_item t7 on cast(m.xhlb as text) = cast(t7."ID" as text)
         left join ods_paas_uf_productline_infov2 as cpx on cpx.cpxbm = m.cpxbm
         left join ods_paas_uf_productclass_infov2 as cpclass on cpclass.cpflbm = m.cpflbm
         left join ods_paas_uf_product_infov2 as cp on cp.cpbm = m.cpbm
;



COMMENT ON COLUMN public.dwd_paas_uf_productmodel_infov2.coabmsxrq IS 'COA编码失效日期';
COMMENT ON COLUMN public.dwd_paas_uf_productmodel_infov2.cpzj IS '产品总监';
COMMENT ON COLUMN public.dwd_paas_uf_productmodel_infov2.cpxt IS '产品形态';
COMMENT ON COLUMN public.dwd_paas_uf_productmodel_infov2.cpx IS '产品线';
COMMENT ON COLUMN public.dwd_paas_uf_productmodel_infov2.cpxbm IS '产品线编码';
COMMENT ON COLUMN public.dwd_paas_uf_productmodel_infov2.cpfl IS '产品分类';
COMMENT ON COLUMN public.dwd_paas_uf_productmodel_infov2.cpflbm IS '产品分类编码';
COMMENT ON COLUMN public.dwd_paas_uf_productmodel_infov2.cpmc IS '产品名称';
COMMENT ON COLUMN public.dwd_paas_uf_productmodel_infov2.cpbm IS '产品编码';
COMMENT ON COLUMN public.dwd_paas_uf_productmodel_infov2.cpmcxsmc IS '产品销售名称';
COMMENT ON COLUMN public.dwd_paas_uf_productmodel_infov2.cpxhbm IS '产品型号编码';
COMMENT ON COLUMN public.dwd_paas_uf_productmodel_infov2.cpxh IS '产品型号';
COMMENT ON COLUMN public.dwd_paas_uf_productmodel_infov2.coabm IS 'COA编码';
COMMENT ON COLUMN public.dwd_paas_uf_productmodel_infov2.coabmzt IS 'COA编码状态';
COMMENT ON COLUMN public.dwd_paas_uf_productmodel_infov2.cpjl IS '型号产品经理';
COMMENT ON COLUMN public.dwd_paas_uf_productmodel_infov2.modedatamodifier IS '修改人ID';
COMMENT ON COLUMN public.dwd_paas_uf_productmodel_infov2.modedatamodifydatetime IS '修改时间';
COMMENT ON COLUMN public.dwd_paas_uf_productmodel_infov2.simpledate IS '时间戳';
COMMENT ON COLUMN public.dwd_paas_uf_productmodel_infov2.cplx IS '产品类型';
COMMENT ON COLUMN public.dwd_paas_uf_productmodel_infov2.gnms IS '功能描述';
COMMENT ON COLUMN public.dwd_paas_uf_productmodel_infov2.cpsmzqzt IS '产品生命周期状态';
COMMENT ON COLUMN public.dwd_paas_uf_productmodel_infov2.tzxseom IS '停止销售EOM日期';
COMMENT ON COLUMN public.dwd_paas_uf_productmodel_infov2.tzqmzceofs IS '停止全面支持EOFS日期';
COMMENT ON COLUMN public.dwd_paas_uf_productmodel_infov2.tzfweos IS '停止服务EOS日期';
COMMENT ON COLUMN public.dwd_paas_uf_productmodel_infov2.xksm IS '许可说明';
COMMENT ON COLUMN public.dwd_paas_uf_productmodel_infov2.pzzd IS '配置指导';
COMMENT ON COLUMN public.dwd_paas_uf_productmodel_infov2.xyqdzx IS '行业渠道属性';
COMMENT ON COLUMN public.dwd_paas_uf_productmodel_infov2.shyhlx IS '适合用户类型';
COMMENT ON COLUMN public.dwd_paas_uf_productmodel_infov2.jldw IS '计量单位';
COMMENT ON COLUMN public.dwd_paas_uf_productmodel_infov2.sfsm IS '是否涉密';
COMMENT ON COLUMN public.dwd_paas_uf_productmodel_infov2.sfxc IS '是否信创';
COMMENT ON COLUMN public.dwd_paas_uf_productmodel_infov2.sfshangm IS '是否商密';
COMMENT ON COLUMN public.dwd_paas_uf_productmodel_infov2.xxxhbb IS '销许型号/版本';
COMMENT ON COLUMN public.dwd_paas_uf_productmodel_infov2.xhlb IS '型号类别';
COMMENT ON COLUMN public.dwd_paas_uf_productmodel_infov2.ggpz IS '规格配置';
COMMENT ON COLUMN public.dwd_paas_uf_productmodel_infov2.kkzsm IS '可扩展说明';
COMMENT ON COLUMN public.dwd_paas_uf_productmodel_infov2.xn IS '性能';
COMMENT ON COLUMN public.dwd_paas_uf_productmodel_infov2.jldwzwmc IS '计量单位中文名称';
