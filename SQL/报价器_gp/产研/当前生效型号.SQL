select tl.name              产品线,
       tp.name              产品名称,
       product_sale_name    产品销售名称,
       special_version      销许型号,
       tm.name              产品型号,
       tm.id                id,
       tm.code              产品型号编码,
       function_desc        功能描述,
       hardware_env         规格说明,
       config_guide         配置指导,
       关键性能,
       hardware_spec        硬件基础规格,
       ext_desc             可扩展说明,
       fit_user_type        适合用户类型,
       fit_scene_desc       适用场景说明,
       eom                  停止销售,
       eofs                 停止全面支持,
       eos                  停止服务,
       tm.category          行业渠道属性,
       product_type         产品类型,
       tm.unit              计量单位,
       is_secret            是否涉密,
       is_itaii             是否信创,
       is_business_secret   是否商密,
       product_manager      产品经理,
       product_manager_code 产品经理工号,
       localization         国产化,
       case when coa_status = 1 then '生效' when coa_status = 0 then '失效' end   coa编码状态,
       tm.status            生命周期状态
from t_model tm
         left join t_product_line tl on tm.pl_id = tl.id
         left join t_product tp on tp.id = tm.pd_id
         left join (select model_id,string_agg(name || value || unit,',')  as 关键性能
                    from t_model_specification
                    where type = 1
                    group by model_id) as t2 on tm.id = t2.model_id
where coa_status = 1


