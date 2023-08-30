select tl.name                                                                                 产品线,
       tp.name                                                                                 产品名称,
       tme.code                                                                                产品型号编码,
       tme.name                                                                                产品型号,
       case when coa_status = 1 then '生效' when coa_status = 0 then '失效' end                coa编码状态,
       tme.function_desc                                                                       型号功能描述,
       tme.hardware_env                                                                        型号规格说明,
       tme.ext_desc                                                                            型号可扩展说明,
       tme.config_guide                                                                        型号配置指导,
       tmu.name                                                                                产品模块,
       tmu.function_desc                                                                       模块功能描述,
       tmu.specification_desc                                                                  模块规格说明,
       trc.config_guide                                                                        模块配置指导,
       trc.choice_value                                                                        模块选择值,
       tmu.type_value                                                                          模块类型,
       trc.min_count                                                                           模块最小数量,
       trc.max_count                                                                           模块最大数量,
       case
           when tmu.category = 10 then '行业'
           when tmu.category = 20 then '渠道'
           end                                                                                 行业渠道属性,
       tme.is_secret                                                                           是否涉密,
       tme.is_itaii                                                                            是否信创,
       tme.fit_user_type                                                                       适合用户类型,
       trc.rule_value                                                                          模块配置规则,
       trc.total_count                                                                         合计不超过数量,
       tmu.material_code                                                                       物料编码,
       case when tpr.status = 1 then '启用' when tpr.status = 0 then '停用' end                价格状态,
       case
           when tpr.approve_status = 10 then '未配置'
           when tpr.approve_status = 20 then '待审批'
           when tpr.approve_status = 30 then '审批拒绝'
           when tpr.approve_status = 40 then '审批通过'
           end                                                                                 审批状态,
       case when tpr.default_price = 1 then '是' when tpr.default_price = 0 then '否' end      默认价格,
       tpt.name                                                                                价格名称,
       tpr.price                                                                               价格,
       case when tpt.name = '阶梯价格' then ("阶梯开始" || '~' || "阶梯结束") else null end as 阶梯区间,
       case when tpt.name = '阶梯价格' then 阶梯价格 else null end                             阶梯价格,
       ttpr.price                                                                              通用配件价格
from t_rule_config trc
         left join t_model tme on trc.model_id = tme.id
         left join t_module tmu on trc.module_id = tmu.id
         left join t_product_line tl on tme.pl_id = tl.id
         left join t_product tp on tme.pd_id = tp.id
         left join (SELECT *,
                           json_array_elements(ladder_price) ->> 'startCount' AS "阶梯开始",
                           json_array_elements(ladder_price) ->> 'endCount'   AS "阶梯结束",
                           json_array_elements(ladder_price) ->> 'price'      AS 阶梯价格
                    FROM t_price
                    where ladder_price is not null and status = 1
                    union all
                    SELECT *,
                           null "阶梯开始",
                           null "阶梯结束",
                           null 阶梯价格
                    FROM t_price
                    where ladder_price is null and status = 1) as tpr on trc.module_id = tpr.module_id and trc.model_id=tpr.model_id
         left join t_price_type tpt on tpt.id = tpr.pt_id
         left join (select * from t_price where model_id = -1) as ttpr on trc.module_id = ttpr.module_id
where trc.del = 0
  and trc.work_area_id != '3'
group by tl.name,
         tp.name,
         tme.code,
         tme.name,
         case when coa_status = 1 then '生效' when coa_status = 0 then '失效' end,
         tme.function_desc,
         tme.hardware_env,
         tme.ext_desc,
         tme.config_guide,
         tmu.name,
         tmu.function_desc,
         tmu.specification_desc,
         trc.config_guide,
         trc.choice_value,
         tmu.type_value,
         trc.min_count,
         trc.max_count,
         case
             when tmu.category = 10 then '行业'
             when tmu.category = 20 then '渠道'
             end,
         tme.is_secret,
         tme.is_itaii,
         tme.fit_user_type,
         trc.rule_value,
         trc.total_count,
         tmu.material_code,
         case when tpr.status = 1 then '启用' when tpr.status = 0 then '停用' end  ,
         case
             when tpr.approve_status = 10 then '未配置'
             when tpr.approve_status = 20 then '待审批'
             when tpr.approve_status = 30 then '审批拒绝'
             when tpr.approve_status = 40 then '审批通过'
             end,
         case when tpr.default_price = 1 then '是' when tpr.default_price = 0 then '否' end,
         tpt.name,
         tpr.price,
         case when tpt.name = '阶梯价格' then ("阶梯开始" || '~' || "阶梯结束") else null end,
         case when tpt.name = '阶梯价格' then 阶梯价格 else null end,
         ttpr.price







--原始
select tp.id             系统id,
       tp.status         状态,
       tme.code          产品型号编码,
       tme.name          产品型号,
       tmu.name          产品模块,
       tmu.material_code 物料编码,
       tpt.name          价格名称,
       tp.price          价格,
       tp.default_price  默认价格,
       tp.approve_status 审批状态,
       tp.module_type    模块类型,
       tmu.category      行业渠道属性,
       tp.ladder_price   阶梯价
from t_price tp
         left join t_model tme on tp.model_id = tme.id
         left join t_module tmu on tp.module_id = tmu.id
         left join t_price_type tpt on tpt.id = tp.pt_id
where tme.name ='DAS-ABL-AiFGAP-1000' and  tmu.material_code='FW030687-00002'



