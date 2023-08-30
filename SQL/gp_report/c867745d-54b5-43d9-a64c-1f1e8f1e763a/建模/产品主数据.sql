select * from ex_ods_pass_ecology_uf_product limit 10


select * from  ex_ods_pass_ecology_uf_productline limit 10

select * from ds_ecology_uf_productline_infoV2 limit 10
select * from ds_ecology_uf_productclass_infoV2 limit 10
select * from ds_ecology_uf_product_infoV2 limit 10
select * from ds_ecology_uf_productmodel_infoV2 limit 10



select cpx_name,cpfl from ds_ecology_uf_productclass_infoV2 as a
       join ods_oa_finance_cp_map_relation as b on a.field0004 = b.item_code

         limit 10