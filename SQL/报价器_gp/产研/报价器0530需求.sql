--同时存在硬一体+硬件平台+硬件配件或通用硬件配件的型号
select 产品型号,  count(distinct type_value) num
from (select trc.id   id,
             tme.code 产品型号编码,
             tme.name 产品型号,
             tmu.name 产品模块,
             type_value
      from t_rule_config trc
               left join t_model tme on trc.model_id = tme.id
               left join t_module tmu on trc.module_id = tmu.id
      where tmu.type_value in ('软硬一体', '硬件平台',  '硬件配件') ) as t
group by 产品型号

-- ('软硬一体', '硬件平台', '硬件配件')
-- ('软硬一体', '硬件平台',  '通用硬件配件')


--同时存在两个及以上  软硬一体或硬件平台
select distinct 产品型号,
                type_value,
                num
from (select 产品型号, type_value, count(distinct 产品模块) num
      from (select trc.id   id,
                   tme.code 产品型号编码,
                   tme.name 产品型号,
                   tmu.name 产品模块,
                   type_value
            from t_rule_config trc
                     left join t_model tme on trc.model_id = tme.id
                     left join t_module tmu on trc.module_id = tmu.id
            where tmu.type_value in ('软硬一体', '硬件平台')
              and trc.del = 0
              and trc.work_area_id != '3') as t
      group by 产品型号, type_value) AS t
where (type_value = '软硬一体' and num >= 2)
   or (type_value = '硬件平台' and num >= 2)

--有2个及以上软件模块，且有许可模块的型号--不用动
select 产品型号,
       count(distinct status) num
from (select 产品型号, case when type_value = '软件' then type_value else '许可' end status, num
      from (select 产品型号, type_value, count(distinct 产品模块) num
            from (select trc.id   id,
                         tme.code 产品型号编码,
                         tme.name 产品型号,
                         tmu.name 产品模块,
                         type_value
                  from t_rule_config trc
                           left join t_model tme on trc.model_id = tme.id
                           left join t_module tmu on trc.module_id = tmu.id
                  where tmu.type_value in ('软件', '许可（不受年限约束）', '许可（受年限约束）', '许可（无年限约束）')
                  and trc.del = 0 and trc.work_area_id != '3'
                  ) as t
            group by 产品型号, type_value) as tt
      where (type_value = '软件' and num >= 2)
         or (type_value like '%许可%')) as t
group by 产品型号



--有2个及以上软硬一体模块的型号
select 产品型号, type_value, count(distinct 产品模块) num
from (select trc.id   id,
             tme.code 产品型号编码,
             tme.name 产品型号,
             tmu.name 产品模块,
             type_value
      from t_rule_config trc
               left join t_model tme on trc.model_id = tme.id
               left join t_module tmu on trc.module_id = tmu.id
      where tmu.type_value in ('软硬一体')) as t
group by 产品型号, type_value