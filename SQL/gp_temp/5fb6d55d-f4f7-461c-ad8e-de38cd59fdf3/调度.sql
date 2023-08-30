select * from ods_gp_job_execute_log where job_result='F'

select *,job_name,job_sql from ods_gp_job_schedule_pool where job_owner='尹书山'
job_name = concat('func_','ads_gp_oa_t_hr_eminf_inf_df')
update ods_gp_job_schedule_pool  set level_sort=201
where job_name = concat('func_','ads_gp_oa_t_hr_eminf_inf_df')

set job_sql='insert into  ads_gp_all_t_cpx_cp_zhibiao_df
    (select usercode                                                                as dateid, --存储为日期分区
       cpxname,
       cpname,
       unnest(array [''合同产品金额'',''合同总数量'',''回款合同数量'',''订单成功率'']) as yfkpi,--指标,
       unnest(array [合同产品金额,合同总数量,回款合同数量,订单成功率])         as values,--指标值
       cp_class
from (select (current_date - interval ''1 days'')           usercode,
             cpxname,
             cp_class,
             cpname,
             sum(hetong_gmv) / 1                       as 合同产品金额,
             sum(hetong_totalnum) / 1                  as 合同总数量,
             sum(hetong_huinum) / 1                    as 回款合同数量,
             sum(hetong_huinum) / sum(hetong_totalnum) as 订单成功率
      from ads_gp_oa_t_hangye_hetong_sales_df
      where date_part(''year'', event_date) = ''2023''
      group by --usercode,
               cpxname,
               cp_class,
               cpname) as tt

union all

select usercode, cpx_name, cpname, 指标, 指标值, cp_class
from (select (current_date - interval ''1 days'') usercode,
             b.cp_name      as                  cpname,
             b.cpx_name,
             cp_class,
             ''产品实际毛利'' as                  指标,
             sum(field0046)                     指标值
      from ex_ods_oa_abv5_formmain_105321 as a
               left join ods_oa_finance_cp_map_relation as b on a.field0004 = b.item_code
      GROUP BY b.cp_name,
               b.cpx_name, cp_class) as t

union all
select usercode,
       t1.cpxname,
       t1.cpname,
       指标,
       指标值,
       cp_class
from (select (current_date - interval ''1 days'') usercode,
             cpxname,
             cp_class,
             cpname,
             ''商机预计成交金额'' as              指标,
             sum(yuji_gmv)      as              指标值
      from ads_gp_oa_t_hangye_shangji_sales_df
      where date_part(''year'', event_date) = ''2023''
      group by --usercode,
               cpxname,
               cp_class,
               cpname) as t1

union all

select (current_date - interval ''1 days'') usercode,
       cpx_name                           cpxname,
       t1.cpname,
       ''产品实际收入''  as                 指标,
       t1.产品实际收入 as                 指标值,
       cp_class
from (select b.cp_name as   cpname,
             b.cpx_name,
             cp_class,
             sum(field0278) 产品实际收入
      from ex_ods_oa_abv5_formmain_105321 as a
               left join ods_oa_finance_cp_map_relation as b on a.field0004 = b.item_code
      GROUP BY b.cp_name,
               b.cpx_name, cp_class) as t1);' --,level_sort=151
where job_name = concat('func_','ads_gp_all_t_cpx_cp_zhibiao_df')





























insert into ods_gp_job_schedule_pool
(job_name,
 job_level,
 level_sort,
 job_sql,
 job_desc,
 job_owner)
values ('func_ads_gp_all_t_cpx_cp_zhibiao_df',
        'ads',
        111,
        'insert into  ads_gp_all_t_cpx_cp_zhibiao_df
    (select usercode                                                                               as dateid, --存储为日期分区
            cpxname,
            cpname,
            unnest(array [''产品实际毛利'',''合同产品金额'',''合同总数量'',''回款合同数量'',''订单成功率'']) as yfkpi,--指标,
            unnest(array [产品实际毛利,合同产品金额,合同总数量,回款合同数量,订单成功率])           as values,--指标值
            cp_class
     from (select (current_date - interval ''1 days'' ) usercode,
                  t1.cpx_name                        cpxname,
                  t1.cp_class,
                  t1.cpname                          cpname,
                  产品实际毛利,
                  合同产品金额,
                  合同总数量,
                  回款合同数量,
                  订单成功率
           from (select b.cp_name as   cpname,
                        b.cpx_name,
                        cp_class,
                        sum(field0046) 产品实际毛利
                 from ex_ods_oa_abv5_formmain_105321 as a
                          left join ods_oa_finance_cp_map_relation as b on a.field0004 = b.item_code
                 GROUP BY b.cp_name,
                          b.cpx_name, cp_class) as t1
                    left join (select cpxname,
                                      cp_class,
                                      cpname,
                                      --sum(cp_maoli) / 1                         as 产品实际毛利,
                                      sum(hetong_gmv) / 1                       as 合同产品金额,
                                      sum(hetong_totalnum) / 1                  as 合同总数量,
                                      sum(hetong_huinum) / 1                    as 回款合同数量,
                                      sum(hetong_huinum) / sum(hetong_totalnum) as 订单成功率
                               from ads_gp_oa_t_hangye_hetong_sales_df
                               where date_part(''year'', event_date) = ''2023''
                               group by --usercode,
                                        cpxname,
                                        cp_class,
                                        cpname) as t2 on t1.cpx_name = t2.cpxname and t1.cpname = t2.cpname) as tt
     union all
     select usercode,
            t1.cpxname,
            t1.cpname,
            指标,
            指标值,
            cp_class
     from (select (current_date - interval ''1 days'' ) usercode,
                  cpxname,
                  cp_class,
                  cpname,
                  ''商机预计成交金额'' as              指标,
                  sum(yuji_gmv)      as              指标值
           from ads_gp_oa_t_hangye_shangji_sales_df
           where date_part(''year'', event_date) = ''2023''
           group by --usercode,
                    cpxname,
                    cp_class,
                    cpname) as t1

     union all

     select (current_date - interval ''1 days'' ) usercode,
            cpx_name                           cpxname,
            t1.cpname,
            ''产品实际收入''  as                 指标,
            t1.产品实际收入 as                 指标值,
            cp_class
     from (select b.cp_name as   cpname,
                  b.cpx_name,
                  cp_class,
                  sum(field0278) 产品实际收入
           from ex_ods_oa_abv5_formmain_105321 as a
                    left join ods_oa_finance_cp_map_relation as b on a.field0004 = b.item_code
           GROUP BY b.cp_name,
                    b.cpx_name, cp_class) as t1);',
        '2023 经营结果数据表',
        '尹书山')