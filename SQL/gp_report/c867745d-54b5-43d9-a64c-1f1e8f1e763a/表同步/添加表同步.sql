select *  from ods_gp_job_table_transfer where table_source='paas'  limit 1



insert into ods_gp_job_table_transfer
values ('paas',
        'uf_productline_infoV2',
        '尹书山',
        '产品基础信息产品线表'),
       ('paas',
        'uf_productclass_infoV2',
        '尹书山',
        '产品基础信息分类表'),
       ('paas',
        'uf_product_infoV2',
        '尹书山',
        '产品基础信息表'),
       ('paas',
        'uf_productmodel_infoV2',
        '尹书山',
        '产品基础信息型号表')