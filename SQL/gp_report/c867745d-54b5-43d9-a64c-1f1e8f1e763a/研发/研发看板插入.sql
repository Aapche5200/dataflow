--每天1点执行一次
create or replace function proc1() returns trigger as
$$

begin
    --①产线员工数量
    delete from ads_gp_oa_t_hr_eminf_inf_df;
    insert into ads_gp_oa_t_hr_eminf_inf_df
    select count(distinct field0002) as yuangong_num --在职员工数
    from ds_oa_formmain_10037 as t1
             left join ds_oa_depart_level_all as t2 on t1.field0200 = cast(t2."部门ID" as text)
    where field0180 = '-7808949530931608431'
      and t2.一级部门 in ('智能检测与终端产品线', '基础安全产品线', 'AiLPHA大数据智能安全产品线', '云产品线');

    --②行业维度 销售及用户数相关数据
    delete from ads_gp_oa_t_hangye_hetong_sales_df;
    insert into ads_gp_oa_t_hangye_hetong_sales_df
    select t1.日期         event_date,
           t1.工号         usercode,
           t2.产品线名称   cpxname,
           t2.产品名称     cpname,
           t1.一级行业     cate_one,
           t1.二级行业     cate_two,
           t1.三级行业     cate_three,
           t1.产品实际毛利 cp_maoli,
           t1.合同产品金额 hetong_gmv,
           t1.合同客户数   hetong_usernum,
           t1.合同总数量   hetong_totalnum,
           t1.回款合同数量 hetong_huinum
    from (select date(field0028)                                                                     日期,
                 field0010                                                                           工号,
                 field0091                                                                           一级行业,
                 field0092                                                                           二级行业,
                 field0275                                                                           三级行业,
                 sum(field0046)                                                                      产品实际毛利,
                 sum(case when (field0029 <> '否' or field0078 <> '其他') then field0016 else 0 end) 合同产品金额,
                 count(distinct field0019)                                                           合同客户数,
                 count(distinct field0002)                                                as         合同总数量,
                 count(distinct (case when (field0278) > 0 then field0002 else null end)) as         回款合同数量
          from ds_oa_formmain_105321
          where date(field0028) >= date('2022-01-01')
            and date(field0028) < date(current_date)
          GROUP BY date(field0028),
                   field0010,
                   field0091,
                   field0092,
                   field0275) as t1
             left join
         (select 员工工号,
                 t2.需求表产品线ID,
                 t2.需求表产品ID,
                 t2.产品名称,
                 t2.产品线名称
          from (select 员工工号,
                       id
                from (select produc as                             id,
                             regexp_split_to_table(scmanager, ',') 员工系统自带ID
                      from ds_ecology_matrixtable_7
                      union all
                      select produc,
                             regexp_split_to_table(jfmanager, ',')
                      from ds_ecology_matrixtable_7
                      union all
                      select produc,
                             regexp_split_to_table(yfmanager, ',')
                      from ds_ecology_matrixtable_7
                      union all
                      select produc,
                             regexp_split_to_table(csmanager, ',')
                      from ds_ecology_matrixtable_7
                      union all
                      select produc,
                             regexp_split_to_table(yfcmanager, ',')
                      from ds_ecology_matrixtable_7
                      union all
                      select produc,
                             regexp_split_to_table(productinspector, ',')
                      from ds_ecology_matrixtable_7
                      union all
                      select produc,
                             regexp_split_to_table(zlfzr, ',')
                      from ds_ecology_matrixtable_7
                      union all
                      select produc,
                             regexp_split_to_table(yfmanmager, ',')
                      from ds_ecology_matrixtable_7) as t1
                         left join

                     (SELECT id      员工系统自带ID,
                             loginid 员工工号
                      FROM ds_ecology_hrmresource
                      GROUP BY id,
                               loginid) as t2 on cast(t1.员工系统自带ID as text) = cast(t2.员工系统自带ID as text)
                group by 员工工号, id) as t1
                   left join
               (select t1.cpx      需求表产品线ID,
                       t1.cpxcpbd  需求表产品ID,
                       t2.cpxmc as 产品线名称,
                       t3.name  as 产品名称
                from ds_ecology_uf_IPDxqgl as t1
                         left join ds_ecology_uf_productline as t2 on t1.cpx = t2.cpxid
                         left join ds_ecology_uf_product as t3 on t1.cpxcpbd = t3.cpid
                group by t1.cpx, t1.cpxcpbd, t2.cpxmc, t3.name) as t2 on t1.id = t2.需求表产品ID) as t2
         on t1.工号 = t2.员工工号;

    --③办事处维度 销售及用户数相关数据
    delete from ads_gp_oa_t_banshichu_hetong_sales_df;
    insert into ads_gp_oa_t_banshichu_hetong_sales_df
    select tt1.日期         event_date,
           tt1.工号         usercode,
           tt2.产品线名称   cpxname,
           tt2.产品名称     cpname,
           tt1.办事处ID     banshichu_id,
           tt1.办事处名称   banshichu_name,
           tt1.产品实际毛利 cp_maoli,
           tt1.合同产品金额 hetong_cp_gmv,
           tt1.合同客户数   hetong_usernum,
           tt1.合同总数量   hetong_totalnum,
           tt1.回款合同数量 hetong_huinum
    from (select date(t1.field0028)                                                                  日期,
                 t1.field0010                                                                        工号,
                 t1.field0183                                                                        办事处ID,
                 t2.name                                                                             办事处名称,
                 sum(t1.field0046)                                                                   产品实际毛利,
                 sum(case when (field0029 <> '否' or field0078 <> '其他') then field0016 else 0 end) 合同产品金额,
                 count(distinct t1.field0019)                                                        合同客户数,
                 count(distinct field0002)                                                      as   合同总数量,
                 count(distinct (case when (t1.field0278) > 0 then t1.field0002 else null end)) as   回款合同数量
          from ds_oa_formmain_105321 as t1
                   left join ds_oa_ORG_UNIT as t2 on t1.field0183 = cast(t2.id as text)
          where date(field0028) >= date('2022-01-01')
            and date(field0028) < date(current_date)
          GROUP BY date(t1.field0028),
                   t1.field0010,
                   t1.field0183,
                   t2.name) as tt1
             left join
         (select 员工工号,
                 t2.需求表产品线ID,
                 t2.需求表产品ID,
                 t2.产品名称,
                 t2.产品线名称
          from (select 员工工号,
                       id
                from (select produc as                             id,
                             regexp_split_to_table(scmanager, ',') 员工系统自带ID
                      from ds_ecology_matrixtable_7
                      union all
                      select produc,
                             regexp_split_to_table(jfmanager, ',')
                      from ds_ecology_matrixtable_7
                      union all
                      select produc,
                             regexp_split_to_table(yfmanager, ',')
                      from ds_ecology_matrixtable_7
                      union all
                      select produc,
                             regexp_split_to_table(csmanager, ',')
                      from ds_ecology_matrixtable_7
                      union all
                      select produc,
                             regexp_split_to_table(yfcmanager, ',')
                      from ds_ecology_matrixtable_7
                      union all
                      select produc,
                             regexp_split_to_table(productinspector, ',')
                      from ds_ecology_matrixtable_7
                      union all
                      select produc,
                             regexp_split_to_table(zlfzr, ',')
                      from ds_ecology_matrixtable_7
                      union all
                      select produc,
                             regexp_split_to_table(yfmanmager, ',')
                      from ds_ecology_matrixtable_7) as t1
                         left join

                     (SELECT id      员工系统自带ID,
                             loginid 员工工号
                      FROM ds_ecology_hrmresource
                      GROUP BY id,
                               loginid) as t2 on cast(t1.员工系统自带ID as text) = cast(t2.员工系统自带ID as text)
                group by 员工工号, id) as t1
                   left join
               (select t1.cpx      需求表产品线ID,
                       t1.cpxcpbd  需求表产品ID,
                       t2.cpxmc as 产品线名称,
                       t3.name  as 产品名称
                from ds_ecology_uf_IPDxqgl as t1
                         left join ds_ecology_uf_productline as t2 on t1.cpx = t2.cpxid
                         left join ds_ecology_uf_product as t3 on t1.cpxcpbd = t3.cpid
                group by t1.cpx, t1.cpxcpbd, t2.cpxmc, t3.name) as t2 on t1.id = t2.需求表产品ID) as tt2
         on tt1.工号 = tt2.员工工号;

    --④行业维度 各层级商机销售
    delete from ads_gp_oa_t_hangye_shangji_sales_df;
    insert into ads_gp_oa_t_hangye_shangji_sales_df
    select tt1.日期         event_date,
           tt1.工号         usercode,
           tt2.产品线名称   cpxname,
           tt2.产品名称     cpname,
           tt1.一级行业名称 cate_one,
           tt1.二级行业名称 cate_two,
           tt1.三级行业名称 cate_three,
           tt1.状态         pro_status,
           tt1.预计成交金额 yuji_gmv
    from (select date(t1.field0018)     日期,
                 t1.field0281           工号,
                 --t1.field0160         一级行业,
                 t3.showvalue           一级行业名称,
                 --t1.field0161         二级行业,
                 t4.showvalue           二级行业名称,
                 --t1.field0282         三级行业,
                 t5.showvalue           三级行业名称,
                 case
                     when t1.field0028 = '5146369105019027477' then '暂停'
                     when t1.field0028 = '2945291516390491342' then '丢单'
                     when t1.field0028 = '-5091227863388631683' then '失效'
                     when t1.field0028 = '-2036947102919343196' then '已签合同'
                     when t1.field0028 = '-4552238618822292475' then '进行中'
                     else '其他' end as 状态,
                 sum(t2.field0046)   as 预计成交金额
          from ds_oa_formmain_2156 t1
                   left join ds_oa_formson_2157 t2 on t1.id = t2.formmain_id
                   left join ds_oa_ctp_enum_item t3 on cast(t1.field0160 as text) = cast(t3.id as text)
                   left join ds_oa_ctp_enum_item t4 on cast(t1.field0161 as text) = cast(t4.id as text)
                   left join ds_oa_ctp_enum_item t5 on cast(t1.field0282 as text) = cast(t5.id as text)
          where date(t1.field0018) >= date('2022-01-01')
          GROUP BY date(t1.field0018),
                   t1.field0281,
                   --t1.field0160,
                   t3.showvalue,
                   --t1.field0161,
                   t4.showvalue,
                   --t1.field0282,
                   t5.showvalue,
                   case
                       when t1.field0028 = '5146369105019027477' then '暂停'
                       when t1.field0028 = '2945291516390491342' then '丢单'
                       when t1.field0028 = '-5091227863388631683' then '失效'
                       when t1.field0028 = '-2036947102919343196' then '已签合同'
                       when t1.field0028 = '-4552238618822292475' then '进行中'
                       else '其他' end) as tt1
             left join
         (select 员工工号,
                 t2.需求表产品线ID,
                 t2.需求表产品ID,
                 t2.产品名称,
                 t2.产品线名称
          from (select 员工工号,
                       id
                from (select produc as                             id,
                             regexp_split_to_table(scmanager, ',') 员工系统自带ID
                      from ds_ecology_matrixtable_7
                      union all
                      select produc,
                             regexp_split_to_table(jfmanager, ',')
                      from ds_ecology_matrixtable_7
                      union all
                      select produc,
                             regexp_split_to_table(yfmanager, ',')
                      from ds_ecology_matrixtable_7
                      union all
                      select produc,
                             regexp_split_to_table(csmanager, ',')
                      from ds_ecology_matrixtable_7
                      union all
                      select produc,
                             regexp_split_to_table(yfcmanager, ',')
                      from ds_ecology_matrixtable_7
                      union all
                      select produc,
                             regexp_split_to_table(productinspector, ',')
                      from ds_ecology_matrixtable_7
                      union all
                      select produc,
                             regexp_split_to_table(zlfzr, ',')
                      from ds_ecology_matrixtable_7
                      union all
                      select produc,
                             regexp_split_to_table(yfmanmager, ',')
                      from ds_ecology_matrixtable_7) as t1
                         left join

                     (SELECT id      员工系统自带ID,
                             loginid 员工工号
                      FROM ds_ecology_hrmresource
                      GROUP BY id,
                               loginid) as t2 on cast(t1.员工系统自带ID as text) = cast(t2.员工系统自带ID as text)
                group by 员工工号, id) as t1
                   left join
               (select t1.cpx      需求表产品线ID,
                       t1.cpxcpbd  需求表产品ID,
                       t2.cpxmc as 产品线名称,
                       t3.name  as 产品名称
                from ds_ecology_uf_IPDxqgl as t1
                         left join ds_ecology_uf_productline as t2 on t1.cpx = t2.cpxid
                         left join ds_ecology_uf_product as t3 on t1.cpxcpbd = t3.cpid
                group by t1.cpx, t1.cpxcpbd, t2.cpxmc, t3.name) as t2 on t1.id = t2.需求表产品ID) as tt2
         on tt1.工号 = tt2.员工工号;

    --⑤办事处维度 各层级商机销售
    delete from ads_gp_oa_t_banshichu_shangji_sales_df;
    insert into ads_gp_oa_t_banshichu_shangji_sales_df
    select tt1.日期         event_date,
           tt1.工号         usercode,
           tt2.产品线名称   cpxname,
           tt2.产品名称     cpname,
           tt1.办事处ID     banshichu_id,
           tt1.办事处名称   banshichu_name,
           tt1.状态         pro_status,
           tt1.预计成交金额 yuji_gmv
    from (select date(t1.field0018)     日期,
                 t1.field0281           工号,
                 t1.field0040           办事处ID,
                 t3.name                办事处名称,
                 case
                     when t1.field0028 = '5146369105019027477' then '暂停'
                     when t1.field0028 = '2945291516390491342' then '丢单'
                     when t1.field0028 = '-5091227863388631683' then '失效'
                     when t1.field0028 = '-2036947102919343196' then '已签合同'
                     when t1.field0028 = '-4552238618822292475' then '进行中'
                     else '其他' end as 状态,
                 sum(t2.field0046)   as 预计成交金额
          from ds_oa_formmain_2156 t1
                   left join ds_oa_formson_2157 t2 on t1.id = t2.formmain_id
                   left join ds_oa_ORG_UNIT as t3 on t1.field0040 = cast(t3.id as text)
          where date(t1.field0018) >= date('2022-01-01')
          GROUP BY date(t1.field0018),
                   t1.field0281,
                   t1.field0040,
                   t3.name,
                   case
                       when t1.field0028 = '5146369105019027477' then '暂停'
                       when t1.field0028 = '2945291516390491342' then '丢单'
                       when t1.field0028 = '-5091227863388631683' then '失效'
                       when t1.field0028 = '-2036947102919343196' then '已签合同'
                       when t1.field0028 = '-4552238618822292475' then '进行中'
                       else '其他' end) as tt1
             left join
         (select 员工工号,
                 t2.需求表产品线ID,
                 t2.需求表产品ID,
                 t2.产品名称,
                 t2.产品线名称
          from (select 员工工号,
                       id
                from (select produc as                             id,
                             regexp_split_to_table(scmanager, ',') 员工系统自带ID
                      from ds_ecology_matrixtable_7
                      union all
                      select produc,
                             regexp_split_to_table(jfmanager, ',')
                      from ds_ecology_matrixtable_7
                      union all
                      select produc,
                             regexp_split_to_table(yfmanager, ',')
                      from ds_ecology_matrixtable_7
                      union all
                      select produc,
                             regexp_split_to_table(csmanager, ',')
                      from ds_ecology_matrixtable_7
                      union all
                      select produc,
                             regexp_split_to_table(yfcmanager, ',')
                      from ds_ecology_matrixtable_7
                      union all
                      select produc,
                             regexp_split_to_table(productinspector, ',')
                      from ds_ecology_matrixtable_7
                      union all
                      select produc,
                             regexp_split_to_table(zlfzr, ',')
                      from ds_ecology_matrixtable_7
                      union all
                      select produc,
                             regexp_split_to_table(yfmanmager, ',')
                      from ds_ecology_matrixtable_7) as t1
                         left join

                     (SELECT id      员工系统自带ID,
                             loginid 员工工号
                      FROM ds_ecology_hrmresource
                      GROUP BY id,
                               loginid) as t2 on cast(t1.员工系统自带ID as text) = cast(t2.员工系统自带ID as text)
                group by 员工工号, id) as t1
                   left join
               (select t1.cpx      需求表产品线ID,
                       t1.cpxcpbd  需求表产品ID,
                       t2.cpxmc as 产品线名称,
                       t3.name  as 产品名称
                from ds_ecology_uf_IPDxqgl as t1
                         left join ds_ecology_uf_productline as t2 on t1.cpx = t2.cpxid
                         left join ds_ecology_uf_product as t3 on t1.cpxcpbd = t3.cpid
                group by t1.cpx, t1.cpxcpbd, t2.cpxmc, t3.name) as t2 on t1.id = t2.需求表产品ID) as tt2
         on tt1.工号 = tt2.员工工号;


    --⑥测试合同数据
    delete from ads_gp_oa_t_ceshi_hetong_qty_df;
    insert into ads_gp_oa_t_ceshi_hetong_qty_df
    select tt1.日期         event_date,
           tt1.工号         usercode,
           tt2.产品线名称   cpxname,
           tt2.产品名称     cpname,
           tt1.测试合同数量 hetong_ceshi_qty
    from (select 日期,
                 工号,
                 count(DISTINCT id) as 测试合同数量
          from (SELECT tt1.日期,
                       tt1.id,
                       tt2.工号
                FROM (SELECT t1.id,
                             date(t1.start_date) as 日期,
                             t2.料品编码
                      FROM ds_oa_formmain_18955 AS t1
                               LEFT JOIN
                           (SELECT formmain_id,
                                   field0060 as 料品编码
                            FROM ds_oa_formson_18956
                            UNION ALL
                            SELECT formmain_id, field0234 as 料品编码
                            FROM ds_oa_formson_19389) AS t2 ON t1.id = t2.formmain_id
                      where date(t1.start_date) >= date('2022-01-01')
                        and date(t1.start_date) < date(current_date)) AS tt1
                         LEFT JOIN
                     (SELECT field0004 as 料品编码,
                             field0010 as 工号
                      FROM ds_oa_formmain_105321
                      GROUP BY field0004, field0010) AS tt2
                     ON tt1.料品编码 = tt2.料品编码) as t
          GROUP BY 日期, 工号) as tt1
             left join
         (select 员工工号,
                 t2.需求表产品线ID,
                 t2.需求表产品ID,
                 t2.产品名称,
                 t2.产品线名称
          from (select 员工工号,
                       id
                from (select produc as                             id,
                             regexp_split_to_table(scmanager, ',') 员工系统自带ID
                      from ds_ecology_matrixtable_7
                      union all
                      select produc,
                             regexp_split_to_table(jfmanager, ',')
                      from ds_ecology_matrixtable_7
                      union all
                      select produc,
                             regexp_split_to_table(yfmanager, ',')
                      from ds_ecology_matrixtable_7
                      union all
                      select produc,
                             regexp_split_to_table(csmanager, ',')
                      from ds_ecology_matrixtable_7
                      union all
                      select produc,
                             regexp_split_to_table(yfcmanager, ',')
                      from ds_ecology_matrixtable_7
                      union all
                      select produc,
                             regexp_split_to_table(productinspector, ',')
                      from ds_ecology_matrixtable_7
                      union all
                      select produc,
                             regexp_split_to_table(zlfzr, ',')
                      from ds_ecology_matrixtable_7
                      union all
                      select produc,
                             regexp_split_to_table(yfmanmager, ',')
                      from ds_ecology_matrixtable_7) as t1
                         left join

                     (SELECT id      员工系统自带ID,
                             loginid 员工工号
                      FROM ds_ecology_hrmresource
                      GROUP BY id,
                               loginid) as t2 on cast(t1.员工系统自带ID as text) = cast(t2.员工系统自带ID as text)
                group by 员工工号, id) as t1
                   left join
               (select t1.cpx      需求表产品线ID,
                       t1.cpxcpbd  需求表产品ID,
                       t2.cpxmc as 产品线名称,
                       t3.name  as 产品名称
                from ds_ecology_uf_IPDxqgl as t1
                         left join ds_ecology_uf_productline as t2 on t1.cpx = t2.cpxid
                         left join ds_ecology_uf_product as t3 on t1.cpxcpbd = t3.cpid
                group by t1.cpx, t1.cpxcpbd, t2.cpxmc, t3.name) as t2 on t1.id = t2.需求表产品ID) as tt2
         on tt1.工号 = tt2.员工工号;

    --⑦RM评审数据
    delete from ads_gp_rm_t_pingsheng_hetong_qty_df;
    insert into ads_gp_rm_t_pingsheng_hetong_qty_df
    select 日期                                                          event_date,
           员工工号                                                      usercode,
           产品线名称                                                    cpxname,
           产品名称                                                      cpname,
           COUNT(distinct id)                                         AS total_num,--总数量,
           count(distinct (CASE WHEN xqzt = 0 THEN id ELSE null END)) AS weichuli_num--未处理需求数量
    from (select tt1.日期,
                 tt1.员工工号,
                 tt2.产品线名称,
                 tt2.产品名称,
                 tt1.id,
                 tt1.xqzt
          from (select t1.日期,
                       t2.员工工号,
                       t1.id,
                       t1.xqzt
                from (select 日期,
                             提交人ID,
                             id,
                             xqzt
                      from (SELECT tjsj              AS 日期,
                                   cast(tjr as text) AS 提交人ID,
                                   id,
                                   xqzt
                            FROM ds_ecology_uf_IPDxqgl
                            GROUP BY tjsj,
                                     cast(tjr as text),
                                     id,
                                     xqzt
                            UNION all
                            SELECT tjsj               AS 日期,
                                   cast(cpjl as text) AS 产品经理ID,
                                   id,
                                   xqzt
                            FROM ds_ecology_uf_IPDxqgl
                            GROUP BY tjsj,
                                     cast(cpjl as text),
                                     id,
                                     xqzt
                            union all
                            SELECT tjsj                   AS 日期,
                                   cast(reviewer as text) AS 评审人ID,
                                   id,
                                   xqzt
                            FROM ds_ecology_uf_IPDxqgl
                            GROUP BY tjsj,
                                     cast(reviewer as text),
                                     id,
                                     xqzt) as t
                      where date(t.日期) >= date('2022-01-01')
                        and date(t.日期) < date(current_date)
                      group by 日期, 提交人ID, id, xqzt) as t1
                         left join (SELECT id      员工系统自带ID,
                                           loginid 员工工号
                                    FROM ds_ecology_hrmresource
                                    GROUP BY id,
                                             loginid) as t2
                                   on cast(t1.提交人ID as text) = cast(t2.员工系统自带ID as text)) as tt1
                   left join
               (select 员工工号,
                       t2.需求表产品线ID,
                       t2.需求表产品ID,
                       t2.产品名称,
                       t2.产品线名称
                from (select 员工工号,
                             id
                      from (select produc as                             id,
                                   regexp_split_to_table(scmanager, ',') 员工系统自带ID
                            from ds_ecology_matrixtable_7
                            union all
                            select produc,
                                   regexp_split_to_table(jfmanager, ',')
                            from ds_ecology_matrixtable_7
                            union all
                            select produc,
                                   regexp_split_to_table(yfmanager, ',')
                            from ds_ecology_matrixtable_7
                            union all
                            select produc,
                                   regexp_split_to_table(csmanager, ',')
                            from ds_ecology_matrixtable_7
                            union all
                            select produc,
                                   regexp_split_to_table(yfcmanager, ',')
                            from ds_ecology_matrixtable_7
                            union all
                            select produc,
                                   regexp_split_to_table(productinspector, ',')
                            from ds_ecology_matrixtable_7
                            union all
                            select produc,
                                   regexp_split_to_table(zlfzr, ',')
                            from ds_ecology_matrixtable_7
                            union all
                            select produc,
                                   regexp_split_to_table(yfmanmager, ',')
                            from ds_ecology_matrixtable_7) as t1
                               left join

                           (SELECT id      员工系统自带ID,
                                   loginid 员工工号
                            FROM ds_ecology_hrmresource
                            GROUP BY id,
                                     loginid) as t2
                           on cast(t1.员工系统自带ID as text) = cast(t2.员工系统自带ID as text)
                      group by 员工工号, id) as t1
                         left join
                     (select t1.cpx      需求表产品线ID,
                             t1.cpxcpbd  需求表产品ID,
                             t2.cpxmc as 产品线名称,
                             t3.name  as 产品名称
                      from ds_ecology_uf_IPDxqgl as t1
                               left join ds_ecology_uf_productline as t2 on t1.cpx = t2.cpxid
                               left join ds_ecology_uf_product as t3 on t1.cpxcpbd = t3.cpid
                      group by t1.cpx, t1.cpxcpbd, t2.cpxmc, t3.name) as t2 on t1.id = t2.需求表产品ID) as tt2
               on tt1.员工工号 = tt2.员工工号) as ttt
    group by 日期,
             员工工号,
             产品线名称,
             产品名称;

    --⑧员工产品线 产品宽表；权限使用
    delete from ads_gp_rm_t_hr_cp_inf_df;
    insert into ads_gp_rm_t_hr_cp_inf_df
    select 员工工号          usercode,
           t2.需求表产品线ID cpxid,
           t2.需求表产品ID   cpid,
           t2.产品名称       cpname,
           t2.产品线名称     cpxname
    from (select 员工工号,
                 id
          from (select produc as                             id,
                       regexp_split_to_table(scmanager, ',') 员工系统自带ID
                from ds_ecology_matrixtable_7
                union all
                select produc,
                       regexp_split_to_table(jfmanager, ',')
                from ds_ecology_matrixtable_7
                union all
                select produc,
                       regexp_split_to_table(yfmanager, ',')
                from ds_ecology_matrixtable_7
                union all
                select produc,
                       regexp_split_to_table(csmanager, ',')
                from ds_ecology_matrixtable_7
                union all
                select produc,
                       regexp_split_to_table(yfcmanager, ',')
                from ds_ecology_matrixtable_7
                union all
                select produc,
                       regexp_split_to_table(productinspector, ',')
                from ds_ecology_matrixtable_7
                union all
                select produc,
                       regexp_split_to_table(zlfzr, ',')
                from ds_ecology_matrixtable_7
                union all
                select produc,
                       regexp_split_to_table(yfmanmager, ',')
                from ds_ecology_matrixtable_7) as t1
                   left join

               (SELECT id      员工系统自带ID,
                       loginid 员工工号
                FROM ds_ecology_hrmresource
                GROUP BY id,
                         loginid) as t2 on cast(t1.员工系统自带ID as text) = cast(t2.员工系统自带ID as text)
          group by 员工工号, id) as t1
             left join
         (select t1.cpx      需求表产品线ID,
                 t1.cpxcpbd  需求表产品ID,
                 t2.cpxmc as 产品线名称,
                 t3.name  as 产品名称
          from ds_ecology_uf_IPDxqgl as t1
                   left join ds_ecology_uf_productline as t2 on t1.cpx = t2.cpxid
                   left join ds_ecology_uf_product as t3 on t1.cpxcpbd = t3.cpid
          group by t1.cpx, t1.cpxcpbd, t2.cpxmc, t3.name) as t2 on t1.id = t2.需求表产品ID;

    ------ITR--------
    --1itr 研发/质量基础data 产品维度
    delete from ads_gp_pg_t_itr_basic_cp_df;
    insert into ads_gp_pg_t_itr_basic_cp_df
        (select a.product_type_name               as cptype,--产品类型,
                case
                    when (a.gmt_create) < date_trunc('week', current_date) and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '1 week') then '第12周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '1 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '2 week') then '第11周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '2 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '3 week') then '第10周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '3 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '4 week') then '第9周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '4 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '5 week') then '第8周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '5 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '6 week') then '第7周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '6 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '7 week') then '第6周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '7 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '8 week') then '第5周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '8 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '9 week') then '第4周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '9 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '10 week') then '第3周'


                    when (a.gmt_create) < date_trunc('week', current_date - interval '10 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '11 week') then '第2周'


                    when (a.gmt_create) < date_trunc('week', current_date - interval '11 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '12 week') then '第1周'
                    else null end                 as event_date,--日期,
                'ITR工单客户数量'                 as yfkpi,--指标,
                count(distinct a.product_line_id) as values--指标值
         from ds_itr_workorder_work_order a
         where a.work_order_status <> 0
           and a.gmt_create >= date_trunc('week', current_date - interval '12 week') -- 1 week,1 month,3 month
           and a.gmt_create < date_trunc('week', current_date)
         group by a.product_type_name,
                  case
                      when (a.gmt_create) < date_trunc('week', current_date) and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '1 week') then '第12周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '1 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '2 week') then '第11周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '2 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '3 week') then '第10周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '3 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '4 week') then '第9周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '4 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '5 week') then '第8周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '5 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '6 week') then '第7周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '6 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '7 week') then '第6周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '7 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '8 week') then '第5周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '8 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '9 week') then '第4周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '9 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '10 week') then '第3周'


                      when (a.gmt_create) < date_trunc('week', current_date - interval '10 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '11 week') then '第2周'


                      when (a.gmt_create) < date_trunc('week', current_date - interval '11 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '12 week') then '第1周'
                      else null end

         union all

         select a.product_type_name  as 产品类型,
                case
                    when (a.gmt_create) < date_trunc('week', current_date) and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '1 week') then '第12周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '1 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '2 week') then '第11周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '2 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '3 week') then '第10周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '3 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '4 week') then '第9周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '4 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '5 week') then '第8周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '5 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '6 week') then '第7周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '6 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '7 week') then '第6周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '7 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '8 week') then '第5周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '8 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '9 week') then '第4周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '9 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '10 week') then '第3周'


                    when (a.gmt_create) < date_trunc('week', current_date - interval '10 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '11 week') then '第2周'


                    when (a.gmt_create) < date_trunc('week', current_date - interval '11 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '12 week') then '第1周'
                    else null end    as 日期,
                'ITR工单数量'        as 指标,
                count(distinct a.id) as 指标值
         from ds_itr_workorder_work_order a
         where a.work_order_status <> 0
           and a.gmt_create >= date_trunc('week', current_date - interval '12 week') -- 1 week,1 month,3 month
           and a.gmt_create < date_trunc('week', current_date)
         group by a.product_type_name,
                  case
                      when (a.gmt_create) < date_trunc('week', current_date) and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '1 week') then '第12周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '1 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '2 week') then '第11周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '2 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '3 week') then '第10周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '3 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '4 week') then '第9周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '4 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '5 week') then '第8周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '5 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '6 week') then '第7周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '6 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '7 week') then '第6周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '7 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '8 week') then '第5周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '8 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '9 week') then '第4周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '9 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '10 week') then '第3周'


                      when (a.gmt_create) < date_trunc('week', current_date - interval '10 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '11 week') then '第2周'


                      when (a.gmt_create) < date_trunc('week', current_date - interval '11 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '12 week') then '第1周'
                      else null end

         union all

         select a.product_type_name    as 产品类型,
                case
                    when (a.gmt_create) < date_trunc('week', current_date) and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '1 week') then '第12周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '1 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '2 week') then '第11周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '2 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '3 week') then '第10周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '3 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '4 week') then '第9周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '4 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '5 week') then '第8周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '5 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '6 week') then '第7周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '6 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '7 week') then '第6周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '7 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '8 week') then '第5周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '8 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '9 week') then '第4周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '9 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '10 week') then '第3周'


                    when (a.gmt_create) < date_trunc('week', current_date - interval '10 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '11 week') then '第2周'


                    when (a.gmt_create) < date_trunc('week', current_date - interval '11 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '12 week') then '第1周'
                    else null end      as 日期,
                'L3软件问题的工单数量' as 指标,
                count(distinct a.id)   as 指标值
         from ds_itr_workorder_work_order a
                  inner join ds_itr_workorder_flow_table b on a.id = b.biz_id
                  inner join ds_itr_workorder_flow_node c on b.id = c.flow_id
         where c.node_name = 'L3处理'
           and a.work_order_status <> 0
           and a.problem_big_type = 1554645069768249346
           and a.gmt_create >= date_trunc('week', current_date - interval '12 week') -- 1 week,1 month,3 month
           and a.gmt_create < date_trunc('week', current_date)
         group by a.product_type_name,
                  case
                      when (a.gmt_create) < date_trunc('week', current_date) and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '1 week') then '第12周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '1 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '2 week') then '第11周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '2 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '3 week') then '第10周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '3 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '4 week') then '第9周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '4 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '5 week') then '第8周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '5 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '6 week') then '第7周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '6 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '7 week') then '第6周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '7 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '8 week') then '第5周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '8 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '9 week') then '第4周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '9 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '10 week') then '第3周'


                      when (a.gmt_create) < date_trunc('week', current_date - interval '10 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '11 week') then '第2周'


                      when (a.gmt_create) < date_trunc('week', current_date - interval '11 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '12 week') then '第1周'
                      else null end

         union all

         select a.product_type_name as 产品类型,
                case
                    when (a.gmt_create) < date_trunc('week', current_date) and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '1 week') then '第12周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '1 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '2 week') then '第11周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '2 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '3 week') then '第10周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '3 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '4 week') then '第9周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '4 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '5 week') then '第8周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '5 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '6 week') then '第7周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '6 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '7 week') then '第6周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '7 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '8 week') then '第5周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '8 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '9 week') then '第4周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '9 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '10 week') then '第3周'


                    when (a.gmt_create) < date_trunc('week', current_date - interval '10 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '11 week') then '第2周'


                    when (a.gmt_create) < date_trunc('week', current_date - interval '11 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '12 week') then '第1周'
                    else null end   as 日期,
                'L3升单闭环率'      as 指标,
                ROUND((count(case when c.node_name = 'L3处理' then 1 else null end) / count(distinct a.id) ::NUMERIC),
                      2)            as 指标值
         from ds_itr_workorder_work_order a
                  inner join ds_itr_workorder_flow_table b on a.id = b.biz_id
                  inner join ds_itr_workorder_flow_node c on b.id = c.flow_id
         where a.work_order_status <> 0
           and a.gmt_create >= date_trunc('week', current_date - interval '12 week') -- 1 week,1 month,3 month
           and a.gmt_create < date_trunc('week', current_date)
         group by a.product_type_name,
                  case
                      when (a.gmt_create) < date_trunc('week', current_date) and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '1 week') then '第12周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '1 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '2 week') then '第11周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '2 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '3 week') then '第10周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '3 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '4 week') then '第9周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '4 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '5 week') then '第8周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '5 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '6 week') then '第7周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '6 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '7 week') then '第6周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '7 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '8 week') then '第5周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '8 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '9 week') then '第4周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '9 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '10 week') then '第3周'


                      when (a.gmt_create) < date_trunc('week', current_date - interval '10 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '11 week') then '第2周'


                      when (a.gmt_create) < date_trunc('week', current_date - interval '11 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '12 week') then '第1周'
                      else null end

         union all

         select a.product_type_name      as 产品类型,
                case
                    when (a.gmt_create) < date_trunc('week', current_date) and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '1 week') then '第12周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '1 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '2 week') then '第11周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '2 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '3 week') then '第10周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '3 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '4 week') then '第9周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '4 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '5 week') then '第8周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '5 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '6 week') then '第7周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '6 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '7 week') then '第6周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '7 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '8 week') then '第5周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '8 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '9 week') then '第4周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '9 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '10 week') then '第3周'


                    when (a.gmt_create) < date_trunc('week', current_date - interval '10 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '11 week') then '第2周'


                    when (a.gmt_create) < date_trunc('week', current_date - interval '11 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '12 week') then '第1周'
                    else null end        as 日期,
                '工单数量'               as 指标,
                count(product_type_name) as 指标值
         from ds_itr_workorder_work_order a
         where a.work_order_status <> 0
           and gmt_create >= date_trunc('week', current_date - interval '12 week') -- 1 week,1 month,3 month
           and gmt_create < date_trunc('week', current_date)
         group by case
                      when (a.gmt_create) < date_trunc('week', current_date) and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '1 week') then '第12周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '1 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '2 week') then '第11周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '2 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '3 week') then '第10周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '3 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '4 week') then '第9周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '4 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '5 week') then '第8周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '5 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '6 week') then '第7周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '6 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '7 week') then '第6周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '7 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '8 week') then '第5周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '8 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '9 week') then '第4周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '9 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '10 week') then '第3周'


                      when (a.gmt_create) < date_trunc('week', current_date - interval '10 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '11 week') then '第2周'


                      when (a.gmt_create) < date_trunc('week', current_date - interval '11 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '12 week') then '第1周'
                      else null end,
                  a.product_type_name);


    --2itr 研发/质量基础data 新增遗留分布
    delete from ads_gp_pg_t_itr_newold_fenbu_df;
    insert into ads_gp_pg_t_itr_newold_fenbu_df
        (select a.product_type_name                                                     as cptype,--产品类型,
                case
                    when (a.gmt_create) < date_trunc('week', current_date) and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '1 week') then '第12周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '1 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '2 week') then '第11周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '2 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '3 week') then '第10周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '3 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '4 week') then '第9周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '4 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '5 week') then '第8周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '5 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '6 week') then '第7周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '6 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '7 week') then '第6周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '7 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '8 week') then '第5周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '8 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '9 week') then '第4周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '9 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '10 week') then '第3周'


                    when (a.gmt_create) < date_trunc('week', current_date - interval '10 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '11 week') then '第2周'


                    when (a.gmt_create) < date_trunc('week', current_date - interval '11 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '12 week') then '第1周'
                    else null end                                                       as event_date,--日期,
                a.product_version                                                       as banben_name,--版本名称,
                count(a.product_line_id)                                                as fankui_pro_qty,--反馈项目总数,
                count(case when a.work_order_status not in (0, 5) then 1 else null end) as cp_wjj_qty,--产品内部未解决数,
                count(case when a.work_order_status not in (0, 5) then 1 else null end) as yiliu_wjj_qty,--遗留未解决项目数一暂定,
                ROUND((count(case when a.work_order_status not in (0, 5) then 1 else null end) /
                       count(distinct a.id) ::NUMERIC),
                      2)                                                                as bihuan_lv--闭环率
         from ds_itr_workorder_work_order a
         where a.work_order_status <> 0
           and a.gmt_create >= date_trunc('week', current_date - interval '12 week') -- 1 week,1 month,3 month
           and a.gmt_create < date_trunc('week', current_date)
         group by a.product_type_name,
                  a.product_version,
                  case
                      when (a.gmt_create) < date_trunc('week', current_date) and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '1 week') then '第12周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '1 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '2 week') then '第11周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '2 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '3 week') then '第10周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '3 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '4 week') then '第9周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '4 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '5 week') then '第8周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '5 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '6 week') then '第7周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '6 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '7 week') then '第6周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '7 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '8 week') then '第5周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '8 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '9 week') then '第4周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '9 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '10 week') then '第3周'


                      when (a.gmt_create) < date_trunc('week', current_date - interval '10 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '11 week') then '第2周'


                      when (a.gmt_create) < date_trunc('week', current_date - interval '11 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '12 week') then '第1周'
                      else null end);


    --3itr 研发/质量基础data 用户问题分布
    delete from ads_gp_pg_t_itr_lastuser_fenbu_df;
    insert into ads_gp_pg_t_itr_lastuser_fenbu_df
        (select a.product_type_name                 as cptype,--产品类型,
                case
                    when (a.gmt_create) < date_trunc('week', current_date) and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '1 week') then '第12周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '1 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '2 week') then '第11周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '2 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '3 week') then '第10周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '3 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '4 week') then '第9周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '4 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '5 week') then '第8周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '5 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '6 week') then '第7周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '6 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '7 week') then '第6周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '7 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '8 week') then '第5周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '8 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '9 week') then '第4周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '9 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '10 week') then '第3周'


                    when (a.gmt_create) < date_trunc('week', current_date - interval '10 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '11 week') then '第2周'


                    when (a.gmt_create) < date_trunc('week', current_date - interval '11 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '12 week') then '第1周'
                    else null end                   as event_date,--日期,
                a.product_version                   as banben_name,--版本名称,
                a.ultimate_customer                 as last_user,--最终客户,
                count(distinct a.ultimate_customer) as gongdan_qty--工单数量
         from ds_itr_workorder_work_order a
         where a.work_order_status <> 0
--    and a.ultimate_customer
         group by a.ultimate_customer,
                  a.product_type_name,
                  case
                      when (a.gmt_create) < date_trunc('week', current_date) and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '1 week') then '第12周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '1 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '2 week') then '第11周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '2 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '3 week') then '第10周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '3 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '4 week') then '第9周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '4 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '5 week') then '第8周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '5 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '6 week') then '第7周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '6 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '7 week') then '第6周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '7 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '8 week') then '第5周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '8 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '9 week') then '第4周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '9 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '10 week') then '第3周'


                      when (a.gmt_create) < date_trunc('week', current_date - interval '10 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '11 week') then '第2周'


                      when (a.gmt_create) < date_trunc('week', current_date - interval '11 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '12 week') then '第1周'
                      else null end,
                  a.product_version);

    --4itr 研发/质量基础data 问题类型分布
    delete from ads_gp_pg_t_itr_qtype_fenbu_df;
    insert into ads_gp_pg_t_itr_qtype_fenbu_df
        (select a.product_type_name as cptype,--产品类型,
                case
                    when (a.gmt_create) < date_trunc('week', current_date) and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '1 week') then '第12周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '1 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '2 week') then '第11周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '2 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '3 week') then '第10周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '3 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '4 week') then '第9周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '4 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '5 week') then '第8周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '5 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '6 week') then '第7周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '6 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '7 week') then '第6周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '7 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '8 week') then '第5周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '8 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '9 week') then '第4周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '9 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '10 week') then '第3周'


                    when (a.gmt_create) < date_trunc('week', current_date - interval '10 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '11 week') then '第2周'


                    when (a.gmt_create) < date_trunc('week', current_date - interval '11 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '12 week') then '第1周'
                    else null end   as event_date,--日期,
                a.product_version   as banben_name,--版本名称,
                b.show_name         as qa_type,--问题类型,
                count(b.show_name)  as cp_qty--产品数量
         from ds_itr_workorder_work_order a
                  inner join ds_itr_workorder_dictionary b on a.problem_small_type = b.id
         where a.work_order_status <> 0
           and a.gmt_create >= date_trunc('week', current_date - interval '12 week') -- 1 week,1 month,3 month
           and a.gmt_create < date_trunc('week', current_date)
         group by a.product_type_name,
                  b.show_name,
                  case
                      when (a.gmt_create) < date_trunc('week', current_date) and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '1 week') then '第12周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '1 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '2 week') then '第11周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '2 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '3 week') then '第10周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '3 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '4 week') then '第9周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '4 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '5 week') then '第8周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '5 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '6 week') then '第7周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '6 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '7 week') then '第6周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '7 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '8 week') then '第5周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '8 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '9 week') then '第4周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '9 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '10 week') then '第3周'


                      when (a.gmt_create) < date_trunc('week', current_date - interval '10 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '11 week') then '第2周'


                      when (a.gmt_create) < date_trunc('week', current_date - interval '11 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '12 week') then '第1周'
                      else null end,
                  a.product_version);

    --5itr 研发/质量基础data 工单等级分布
    delete from ads_gp_pg_t_itr_gongdan_fenbu_df;
    insert into ads_gp_pg_t_itr_gongdan_fenbu_df
        (select a.product_type_name as cptype,--产品类型,
                case
                    when (a.gmt_create) < date_trunc('week', current_date) and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '1 week') then '第12周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '1 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '2 week') then '第11周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '2 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '3 week') then '第10周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '3 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '4 week') then '第9周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '4 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '5 week') then '第8周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '5 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '6 week') then '第7周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '6 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '7 week') then '第6周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '7 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '8 week') then '第5周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '8 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '9 week') then '第4周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '9 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '10 week') then '第3周'


                    when (a.gmt_create) < date_trunc('week', current_date - interval '10 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '11 week') then '第2周'


                    when (a.gmt_create) < date_trunc('week', current_date - interval '11 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '12 week') then '第1周'
                    else null end   as event_date,--日期,
                a.product_version   as banben_inf,--版本信息,
                b.show_name         as gongdan_level,--工单等级,
                count(b.show_name)  as gongdan_qty--工单数量
         from ds_itr_workorder_work_order a
                  inner join ds_itr_workorder_dictionary b on a.order_level = b.id
         where a.work_order_status <> 0 --    and
               --    a.gmt_create >= '2022-12-02 00:00:00'
               --    and a.gmt_create <= '2022-12-08 23:00:59'
         group by a.product_type_name,
                  b.show_name,
                  case
                      when (a.gmt_create) < date_trunc('week', current_date) and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '1 week') then '第12周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '1 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '2 week') then '第11周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '2 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '3 week') then '第10周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '3 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '4 week') then '第9周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '4 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '5 week') then '第8周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '5 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '6 week') then '第7周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '6 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '7 week') then '第6周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '7 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '8 week') then '第5周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '8 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '9 week') then '第4周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '9 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '10 week') then '第3周'


                      when (a.gmt_create) < date_trunc('week', current_date - interval '10 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '11 week') then '第2周'


                      when (a.gmt_create) < date_trunc('week', current_date - interval '11 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '12 week') then '第1周'
                      else null end,
                  a.product_version);


    --6itr 研发/质量基础data 产品问题趋势
    delete from ads_gp_pg_t_itr_cp_qushi_df;
    insert into ads_gp_pg_t_itr_cp_qushi_df
        (select a.product_type_name  as cptype,--产品类型,
                case
                    when (a.gmt_create) < date_trunc('week', current_date) and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '1 week') then '第12周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '1 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '2 week') then '第11周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '2 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '3 week') then '第10周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '3 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '4 week') then '第9周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '4 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '5 week') then '第8周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '5 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '6 week') then '第7周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '6 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '7 week') then '第6周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '7 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '8 week') then '第5周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '8 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '9 week') then '第4周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '9 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '10 week') then '第3周'


                    when (a.gmt_create) < date_trunc('week', current_date - interval '10 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '11 week') then '第2周'


                    when (a.gmt_create) < date_trunc('week', current_date - interval '11 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '12 week') then '第1周'
                    else null end    as event_date,--日期,
                count(distinct a.id) as cp_new_qty,--产品新增问题数,
                count(
                        case
                            when a.work_order_status not in (0, 5) then 1
                            else null
                            end
                    )                as cp_weijj_qty,--产品内部未解决数,
                ROUND(
                            count(
                                    case
                                        when a.work_order_status = 5 then 1
                                        else null
                                        end
                                ) / count(DISTINCT a.id):: NUMERIC,
                            2
                    )                as qa_lv--问题闭环率
         from ds_itr_workorder_work_order a
         where a.work_order_status <> 0
           and a.gmt_create >= date_trunc('week', current_date - interval '12 week') -- 1 week,1 month,3 month
           and a.gmt_create < date_trunc('week', current_date)
         group by case
                      when (a.gmt_create) < date_trunc('week', current_date) and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '1 week') then '第12周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '1 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '2 week') then '第11周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '2 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '3 week') then '第10周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '3 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '4 week') then '第9周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '4 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '5 week') then '第8周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '5 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '6 week') then '第7周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '6 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '7 week') then '第6周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '7 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '8 week') then '第5周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '8 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '9 week') then '第4周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '9 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '10 week') then '第3周'


                      when (a.gmt_create) < date_trunc('week', current_date - interval '10 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '11 week') then '第2周'


                      when (a.gmt_create) < date_trunc('week', current_date - interval '11 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '12 week') then '第1周'
                      else null end,
                  a.product_type_name);

    --7itr 客户行业分布
    delete from ads_gp_pg_t_itr_user_hangye_df;
    insert into ads_gp_pg_t_itr_user_hangye_df
        (with formson_table as (select id, formmain_id
                                from ds_oa_formson_1939
                                union all
                                select id, formmain_id
                                from ds_oa_formson_18956
                                union all
                                select id, formmain_id
                                from ds_oa_formson_19389)
         select a.product_type_name               as cptype,--产品类型,
                a.product_version                 as banben_name,--版本名称
                case
                    when (a.gmt_create) < date_trunc('week', current_date) and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '1 week') then '第12周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '1 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '2 week') then '第11周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '2 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '3 week') then '第10周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '3 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '4 week') then '第9周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '4 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '5 week') then '第8周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '5 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '6 week') then '第7周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '6 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '7 week') then '第6周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '7 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '8 week') then '第5周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '8 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '9 week') then '第4周'

                    when (a.gmt_create) < date_trunc('week', current_date - interval '9 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '10 week') then '第3周'


                    when (a.gmt_create) < date_trunc('week', current_date - interval '10 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '11 week') then '第2周'


                    when (a.gmt_create) < date_trunc('week', current_date - interval '11 week') and
                         (a.gmt_create) >= date_trunc('week', current_date - interval '12 week') then '第1周'
                    else null end                 as event_date,--日期,
                --a.product_line_id                 as 合同号一需要匹配行业,
                c.field0380                       as first_cate,--第一行业,
                c.field0381                       as second_cate,--第二行业,
                count(distinct a.product_line_id) as gongdan_qty--工单数量
         from ds_itr_workorder_work_order a
                  inner join formson_table b on product_line_id = cast(b.id as varchar(64))
                  inner join ds_oa_formmain_1119 c on b.formmain_id = c.id
         where a.work_order_status <> 0
--   and a.gmt_create >= date_trunc('month', current_date - interval '1 month') -- 1 week,1 month,3 month
--   and a.gmt_create < date_trunc('month', current_date)
         group by a.product_type_name,
                  a.product_version,
                  --a.product_line_id,
                  case
                      when (a.gmt_create) < date_trunc('week', current_date) and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '1 week') then '第12周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '1 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '2 week') then '第11周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '2 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '3 week') then '第10周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '3 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '4 week') then '第9周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '4 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '5 week') then '第8周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '5 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '6 week') then '第7周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '6 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '7 week') then '第6周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '7 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '8 week') then '第5周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '8 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '9 week') then '第4周'

                      when (a.gmt_create) < date_trunc('week', current_date - interval '9 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '10 week') then '第3周'


                      when (a.gmt_create) < date_trunc('week', current_date - interval '10 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '11 week') then '第2周'


                      when (a.gmt_create) < date_trunc('week', current_date - interval '11 week') and
                           (a.gmt_create) >= date_trunc('week', current_date - interval '12 week') then '第1周'
                      else null end,
                  c.field0380,
                  c.field0381);

end
$$
    language 'plpgsql';


--每天4点执行一次
create or replace function proc2() returns trigger as
$$
begin
    --⑨2023 经营结果数据表
    delete from ads_gp_all_t_cpx_cp_zhibiao_df;
    insert into ads_gp_all_t_cpx_cp_zhibiao_df
        (select usercode,
                cpxname,
                cpname,
                unnest(array ['产品实际毛利','合同产品金额','合同总数量','回款合同数量','订单成功率']) as yfkpi,--指标,
                unnest(array [产品实际毛利,合同产品金额,合同总数量,回款合同数量,订单成功率])           as values--指标值
         from (select usercode,
                      cpxname,
                      cpname,
                      sum(cp_maoli) / 1                         as 产品实际毛利,
                      sum(hetong_gmv) / 1                       as 合同产品金额,
                      sum(hetong_totalnum) / 1                  as 合同总数量,
                      sum(hetong_huinum) / 1                    as 回款合同数量,
                      sum(hetong_huinum) / sum(hetong_totalnum) as 订单成功率
               from ads_gp_oa_t_hangye_hetong_sales_df
               where date_part('year', event_date) = '2023'
               group by usercode,
                        cpxname,
                        cpname) as tt
         union all
         select usercode,
                cpxname,
                cpname,
                '商机预计成交金额' as 指标,
                sum(yuji_gmv)      as 指标值
         from ads_gp_oa_t_hangye_shangji_sales_df
         where date_part('year', event_date) = '2023'
         group by usercode,
                  cpxname,
                  cpname
         union all
         select t1.工号,
                t2.产品线名称,
                t2.产品名称,
                '产品实际收入'  as 指标,
                t1.产品实际收入 as 指标值
         from (select field0010      工号,
                      sum(field0278) 产品实际收入
               from ds_oa_formmain_105321
               GROUP BY field0010) as t1
                  left join
              (select 员工工号,
                      t2.需求表产品线ID,
                      t2.需求表产品ID,
                      t2.产品名称,
                      t2.产品线名称
               from (select 员工工号,
                            id
                     from (select produc as                             id,
                                  regexp_split_to_table(scmanager, ',') 员工系统自带ID
                           from ds_ecology_matrixtable_7
                           union all
                           select produc,
                                  regexp_split_to_table(jfmanager, ',')
                           from ds_ecology_matrixtable_7
                           union all
                           select produc,
                                  regexp_split_to_table(yfmanager, ',')
                           from ds_ecology_matrixtable_7
                           union all
                           select produc,
                                  regexp_split_to_table(csmanager, ',')
                           from ds_ecology_matrixtable_7
                           union all
                           select produc,
                                  regexp_split_to_table(yfcmanager, ',')
                           from ds_ecology_matrixtable_7
                           union all
                           select produc,
                                  regexp_split_to_table(productinspector, ',')
                           from ds_ecology_matrixtable_7
                           union all
                           select produc,
                                  regexp_split_to_table(zlfzr, ',')
                           from ds_ecology_matrixtable_7
                           union all
                           select produc,
                                  regexp_split_to_table(yfmanmager, ',')
                           from ds_ecology_matrixtable_7) as t1
                              left join

                          (SELECT id      员工系统自带ID,
                                  loginid 员工工号
                           FROM ds_ecology_hrmresource
                           GROUP BY id,
                                    loginid) as t2 on cast(t1.员工系统自带ID as text) = cast(t2.员工系统自带ID as text)
                     group by 员工工号, id) as t1
                        left join
                    (select t1.cpx      需求表产品线ID,
                            t1.cpxcpbd  需求表产品ID,
                            t2.cpxmc as 产品线名称,
                            t3.name  as 产品名称
                     from ds_ecology_uf_IPDxqgl as t1
                              left join ds_ecology_uf_productline as t2 on t1.cpx = t2.cpxid
                              left join ds_ecology_uf_product as t3 on t1.cpxcpbd = t3.cpid
                     group by t1.cpx, t1.cpxcpbd, t2.cpxmc, t3.name) as t2 on t1.id = t2.需求表产品ID) as t2
              on t1.工号 = t2.员工工号);
end
$$
    language 'plpgsql';



--模板
create or replace function proc1(num1 int, num2 int, opr char(1), out num int) as
$$
begin
    if opr = '-' then
        num := num1 - num2;
    elseif opr = '+' then
        num := num1 + num2;
    elseif opr = '*' then
        num := num1 * num2;
    elseif opr = '/' then
        num := num1 / num2;
    else
        raise exception 'opr参数值只能为（-，+，*，/）：当前opr的值：%', opr;
    end if;

    insert into t1 select count(*) from t1;

end
$$
    language 'plpgsql'


--定时触发











