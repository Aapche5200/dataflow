--产品维度基础信息表
select * from dws_product_info_detail limit 1
truncate table dws_product_info_detail;
insert into dws_product_info_detail
select cp.cpxbm,     --产品线编码
       cpx.cpx pro_line_name,--产品线名称
       cp.cpflbm,      --产品分类
       cpclass.cpfl,--产品分类名称
       cp.cpbm,      --产品编码
       cp.cpmc,      --产品名称
       cp.cpzj,      --产品总监
       t1.lastname as director_name,--产品总监姓名
       cp.cpjl,      --产品经理
       t2.lastname as manager_name,--产品经理姓名
       to_char(to_timestamp(cast(cp.simpledate as int8)/1000),'YYYY-MM-DD') as event_date--更新时间
from ods_paas_uf_product_infov2 as cp
left join ex_ods_pass_ecology_hrmresource as t1 on cp.cpzj=t1.id
left join ex_ods_pass_ecology_hrmresource as t2 on cp.cpjl=t2.id
left join ods_paas_uf_productline_infov2 as cpx on cpx.cpxbm=cp.cpxbm
left join ods_paas_uf_productclass_infov2 as cpclass on cpclass.cpflbm=cp.cpflbm
;





COMMENT ON COLUMN public.dwd_paas_uf_product_infov2.cpxbm IS '产品线编码';
COMMENT ON COLUMN public.dwd_paas_uf_product_infov2.cpfl IS '产品分类';
COMMENT ON COLUMN public.dwd_paas_uf_product_infov2.cpbm IS '产品编码';
COMMENT ON COLUMN public.dwd_paas_uf_product_infov2.cpmc IS '产品名称';
COMMENT ON COLUMN public.dwd_paas_uf_product_infov2.cpzj IS '产品总监';
COMMENT ON COLUMN public.dwd_paas_uf_product_infov2.cpjl IS '产品经理';
COMMENT ON COLUMN public.dwd_paas_uf_product_infov2.cpxb IS '产品线';
COMMENT ON COLUMN public.dwd_paas_uf_product_infov2.cpflbm IS '产品分类编码';
COMMENT ON COLUMN public.dwd_paas_uf_product_infov2.simpledate IS '更新时间';



--select outkey,* from ex_ods_pass_ecology_hrmresource where workcode = 'AH10228'