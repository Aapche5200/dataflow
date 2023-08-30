--每天1点执行一次
create or replace function proc1() returns trigger as
$$

begin
    --①产线员工数量
    --delete select * from ads_gp_oa_t_hr_eminf_inf_df where date(dateid)=date('2023-03-30');
    insert into ads_gp_oa_t_hr_eminf_inf_df
    select tt2.cpxname                            as depart_cpx,
           count(distinct field0002)              as yuangong_num,
           date(current_date - interval '1 days') as dateid,
           null                                   as none_two
    from (select t2.一级部门 as depart_cpx,
                 t1.field0002
          from ds_oa_formmain_10037 as t1
                   join ds_oa_depart_level_all as t2 on t1.field0200 = cast(t2."部门ID" as text)
          where field0180 = '-7808949530931608431'
            and t2.一级部门 in ('智能检测与终端产品线', '基础安全产品线', 'AiLPHA大数据智能安全产品线', '云产品线')
          group by t1.field0002, t2.一级部门) as tt1
             left join ads_gp_rm_t_hr_cp_inf_df as tt2 on tt1.field0002 = tt2.usercode
    group by tt2.cpxname;

    --②行业维度 销售及用户数相关数据
    delete from ads_gp_oa_t_hangye_hetong_sales_df;
    insert into ads_gp_oa_t_hangye_hetong_sales_df
    select t1.日期         event_date,
           '工号'          usercode,
           '产品线名称'    cpxname,
           t1.cpname       cpname,
           t1.一级行业     cate_one,
           t1.二级行业     cate_two,
           t1.三级行业     cate_three,
           t1.产品实际毛利 cp_maoli,
           t1.合同产品金额 hetong_gmv,
           t1.合同客户数   hetong_usernum,
           t1.合同总数量   hetong_totalnum,
           t1.回款合同数量 hetong_huinum
    from (select date(field0028)                                                                     日期,
                 --field0010                                                                           工号,
                 field0091                                                                           一级行业,
                 field0092                                                                           二级行业,
                 field0275                                                                           三级行业,
                 field0067                                                                as         cpname,
                 sum(field0046)                                                                      产品实际毛利,
                 sum(case when (field0029 <> '否' or field0078 <> '其他') then field0016 else 0 end) 合同产品金额,
                 count(distinct field0019)                                                           合同客户数,
                 count(distinct field0002)                                                as         合同总数量,
                 count(distinct (case when (field0278) > 0 then field0002 else null end)) as         回款合同数量
          from ds_oa_formmain_105321
          where date(field0028) >= date('2022-01-01')
            and date(field0028) < date(current_date)
          GROUP BY date(field0028),
                   --field0010,
                   field0091,
                   field0092,
                   field0275, field0067) as t1;

    --③办事处维度 销售及用户数相关数据
    delete from ads_gp_oa_t_banshichu_hetong_sales_df;
    insert into ads_gp_oa_t_banshichu_hetong_sales_df
    select tt1.日期         event_date,
           '工号'           usercode,
           '产品线名称'     cpxname,
           tt1.cpname       cpname,
           tt1.办事处ID     banshichu_id,
           tt1.办事处名称   banshichu_name,
           tt1.产品实际毛利 cp_maoli,
           tt1.合同产品金额 hetong_cp_gmv,
           tt1.合同客户数   hetong_usernum,
           tt1.合同总数量   hetong_totalnum,
           tt1.回款合同数量 hetong_huinum
    from (select date(t1.field0028)                                                                  日期,
                 --t1.field0010                                                                        工号,
                 t1.field0183                                                                        办事处ID,
                 t2.name                                                                             办事处名称,
                 field0067                                                                      as   cpname,
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
                   --t1.field0010,
                   t1.field0183,
                   t2.name, field0067) as tt1;

    --④行业维度 各层级商机销售
    delete from ads_gp_oa_t_hangye_shangji_sales_df;
    insert into ads_gp_oa_t_hangye_shangji_sales_df
    select tt1.日期         event_date,
           win_lv           usercode,
           tt1.产品id       cpxname,
           tt1.产品名称     cpname,
           tt1.一级行业名称 cate_one,
           tt1.二级行业名称 cate_two,
           tt1.三级行业名称 cate_three,
           tt1.状态         pro_status,
           tt1.预计成交金额 yuji_gmv
    from (select date(t1.field0018)     日期,
                 --t1.field0281           工号,
                 --t1.field0160         一级行业,
                 t3.showvalue           一级行业名称,
                 --t1.field0161         二级行业,
                 t4.showvalue           二级行业名称,
                 --t1.field0282         三级行业,
                 t5.showvalue           三级行业名称,
                 t6.field0005           产品id,
                 t6.field0006           产品名称,
                 t1.field0017        as win_lv,--赢单率
                 case
                     when t1.field0028 = '5146369105019027477' then '暂停'
                     when t1.field0028 = '2945291516390491342' then '丢单'
                     when t1.field0028 = '-5091227863388631683' then '失效'
                     when t1.field0028 = '-2036947102919343196' then '已签合同'
                     when t1.field0028 = '-4552238618822292475' then '进行中'
                     else '其他' end as 状态,
                 sum(t2.field0046)*10000   as 预计成交金额
          from ds_oa_formmain_2156 t1
                   left join ds_oa_formson_2157 t2 on t1.id = t2.formmain_id
                   left join ds_oa_ctp_enum_item t3 on cast(t1.field0160 as text) = cast(t3.id as text)
                   left join ds_oa_ctp_enum_item t4 on cast(t1.field0161 as text) = cast(t4.id as text)
                   left join ds_oa_ctp_enum_item t5 on cast(t1.field0282 as text) = cast(t5.id as text)
                   left join ds_oa_formmain_125708 t6 on t2.field0044 = t6.field0010
          where date(t1.field0018) >= date('2022-01-01')
            and t1.field0286 is null
          GROUP BY date(t1.field0018),
                   --t1.field0281,
                   --t1.field0160
                   t3.showvalue,
                   --t1.field0161,
                   t4.showvalue,
                   --t1.field0282,
                   t5.showvalue, t6.field0005,
                   t6.field0006, t1.field0017,
                   case
                       when t1.field0028 = '5146369105019027477' then '暂停'
                       when t1.field0028 = '2945291516390491342' then '丢单'
                       when t1.field0028 = '-5091227863388631683' then '失效'
                       when t1.field0028 = '-2036947102919343196' then '已签合同'
                       when t1.field0028 = '-4552238618822292475' then '进行中'
                       else '其他' end) as tt1;

    --⑤办事处维度 各层级商机销售
    delete from ads_gp_oa_t_banshichu_shangji_sales_df;
    insert into ads_gp_oa_t_banshichu_shangji_sales_df
    select tt1.日期         event_date,
           win_lv           usercode,
           tt1.产品id       cpxname,
           tt1.产品名称     cpname,
           tt1.办事处ID     banshichu_id,
           tt1.办事处名称   banshichu_name,
           tt1.状态         pro_status,
           tt1.预计成交金额 yuji_gmv
    from (select date(t1.field0018)     日期,
                 --t1.field0281           工号,
                 t1.field0040           办事处ID,
                 t3.name                办事处名称,
                 t4.field0005           产品id,
                 t4.field0006           产品名称,
                 t1.field0017        as win_lv,--赢单率
                 case
                     when t1.field0028 = '5146369105019027477' then '暂停'
                     when t1.field0028 = '2945291516390491342' then '丢单'
                     when t1.field0028 = '-5091227863388631683' then '失效'
                     when t1.field0028 = '-2036947102919343196' then '已签合同'
                     when t1.field0028 = '-4552238618822292475' then '进行中'
                     else '其他' end as 状态,
                 sum(t2.field0046)*10000   as 预计成交金额
          from ds_oa_formmain_2156 t1
                   left join ds_oa_formson_2157 t2 on t1.id = t2.formmain_id
                   left join ds_oa_ORG_UNIT as t3 on t1.field0040 = cast(t3.id as text)
                   left join ds_oa_formmain_125708 t4 on t4.field0010 = t2.field0044
          where date(t1.field0018) >= date('2022-01-01')
          GROUP BY date(t1.field0018),
                   --t1.field0281,
                   t1.field0040,
                   t3.name,
                   t4.field0005,
                   t4.field0006, t1.field0017,
                   case
                       when t1.field0028 = '5146369105019027477' then '暂停'
                       when t1.field0028 = '2945291516390491342' then '丢单'
                       when t1.field0028 = '-5091227863388631683' then '失效'
                       when t1.field0028 = '-2036947102919343196' then '已签合同'
                       when t1.field0028 = '-4552238618822292475' then '进行中'
                       else '其他' end) as tt1;

    --⑥测试合同数据
    delete from ads_gp_oa_t_ceshi_hetong_qty_df;
    insert into ads_gp_oa_t_ceshi_hetong_qty_df
    select tt1.日期         event_date,
           '工号'           usercode,
           '产品线名称'     cpxname,
           tt1.cpname       cpname,
           tt1.测试合同数量 hetong_ceshi_qty
    from (select 日期, cpname, count(distinct id) as 测试合同数量
          from (SELECT t1.id,
                       date(t1.start_date) as 日期,
                       t2.cpname
                FROM ds_oa_formmain_18955 AS t1
                         LEFT JOIN
                     (SELECT formmain_id,
                             field0060 as 料品编码,
                             field0051 as cpname
                      FROM ds_oa_formson_18956
                      where field0051 is not null
                      UNION ALL
                      SELECT formmain_id, field0234 as 料品编码, field0071
                      FROM ds_oa_formson_19389
                      where field0071 is not null) AS t2 ON t1.id = t2.formmain_id
                where date(t1.start_date) >= date('2022-01-01')
                  and date(t1.start_date) < date(current_date)) as tt
          group by 日期, cpname) tt1;

    --⑦RM评审数据
    delete from ads_gp_rm_t_pingsheng_hetong_qty_df;
    insert into ads_gp_rm_t_pingsheng_hetong_qty_df
    select tjsj                                                          event_date,
           '员工工号'                                                    usercode,
           产品线名称                                                    cpxname,
           产品名称                                                      cpname,
           COUNT(distinct id)                                         AS total_num,--总数量,
           count(distinct (CASE WHEN xqzt = 0 THEN id ELSE null END)) AS weichuli_num--未处理需求数量
    from (select regexp_split_to_table(concat_ws(',', t1.tjr, t1.cpjl, t1.reviewer), ',') as usercode,
                 tjsj, --日期
                 t1.id,
                 t1.xqzt,
                 t1.cpx                                                                      需求表产品线ID,
                 t1.cpxcpbd                                                                  需求表产品ID,
                 t2.cpxmc                                                                 as 产品线名称,
                 t3.name                                                                  as 产品名称
          from ds_ecology_uf_IPDxqgl as t1
                   left join ds_ecology_uf_productline as t2 on t1.cpx = t2.cpxid
                   left join ds_ecology_uf_product as t3 on t1.cpxcpbd = t3.cpid
          where date(t1.tjsj) >= date('2022-01-01')
            and date(t1.tjsj) < date(current_date)
          group by regexp_split_to_table(concat_ws(',', t1.tjr, t1.cpjl, t1.reviewer), ','),
                   t1.tjsj, t1.id, t1.xqzt, t1.cpx, t1.cpxcpbd, t2.cpxmc, t3.name) as tt
    group by tjsj, 产品线名称, 产品名称;

    --⑧员工产品线 产品宽表；权限使用
    delete from ads_gp_rm_t_hr_cp_inf_df;
    insert into ads_gp_rm_t_hr_cp_inf_df
    select 员工工号          usercode,
           em_name,--员工姓名
           t2.需求表产品线ID cpxid,
           t2.需求表产品ID   cpid,
           t2.产品名称       cpname,
           t2.产品线名称     cpxname
    from (select 员工工号,
                 em_name,
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

               (SELECT a.id        员工系统自带ID,
                       a.loginid   员工工号,
                       b.field0006 em_name
                FROM ds_ecology_hrmresource as a
                         left join ds_oa_formmain_10037 as b on a.loginid = b.field0002
                GROUP BY a.id,
                         a.loginid, b.field0006) as t2
               on cast(t1.员工系统自带ID as text) = cast(t2.员工系统自带ID as text)
          group by 员工工号, em_name, id) as t1
             left join
         (select t1.cpxid 需求表产品线ID
               , t1.cpxmc 产品线名称
               , t2.cpid  需求表产品ID
               , t2.name  产品名称
          from ds_ecology_uf_productline as t1
                   left join ds_ecology_uf_product as t2 on t1.cpxid = t2.linaname
          group by t1.cpxid, t1.cpxmc, t2.cpid, t2.name) as t2 on t1.id = t2.需求表产品ID
    where 员工工号 is not null
    group by 员工工号,
             em_name,
             t2.需求表产品线ID,
             t2.需求表产品ID,
             t2.产品名称,
             t2.产品线名称;

        --⑨商机项目预测状态数据
    delete from ads_oa_t_shangji_status_sales_df;
    insert into ads_oa_t_shangji_status_sales_df
    select tt1.日期         event_date,
           tt1.产品id       cpid,
           tt1.产品名称     cpname,
           tt1.pro_tag,--项目预测
           tt1.预计成交金额 yuji_gmv,
           null             none_one,
           null             none_two,
           null             none_three
    from (select date(t1.field0018)           日期,
                 t6.field0005                 产品id,
                 t6.field0006                 产品名称,
                 case
                     when t1.field0014 in ('5937608534944795120') then '输单/暂停'
                     when t1.field0014 in ('-913553023266763785', '-8233068084558609166') then '争取'
                     when t1.field0014 in ('1841219522854799953') then '中标'
                     when t1.field0014 in ('-8517000066082717530', '8209823446584085971') then '商机'
                     when t1.field0014 in ('1420566586594969807') then '承诺'
                     when t1.field0014 in ('-6786166406896401062', '8207352573662219079') then '信息'
                     when t1.field0014 in ('-7497924421957762614') then '签订合同'
                     else '其他' end       as pro_tag,
                 sum(t2.field0046) * 10000 as 预计成交金额
          from ds_oa_formmain_2156 t1
                   left join ds_oa_formson_2157 t2 on t1.id = t2.formmain_id
                   left join ds_oa_formmain_125708 t6 on t2.field0044 = t6.field0010
          where date(t1.field0018) >= date('2022-01-01')
            and t1.field0286 is null
          GROUP BY date(t1.field0018),
                   t6.field0005,
                   t6.field0006,
                   case
                       when t1.field0014 in ('5937608534944795120') then '输单/暂停'
                       when t1.field0014 in ('-913553023266763785', '-8233068084558609166') then '争取'
                       when t1.field0014 in ('1841219522854799953') then '中标'
                       when t1.field0014 in ('-8517000066082717530', '8209823446584085971') then '商机'
                       when t1.field0014 in ('1420566586594969807') then '承诺'
                       when t1.field0014 in ('-6786166406896401062', '8207352573662219079') then '信息'
                       when t1.field0014 in ('-7497924421957762614') then '签订合同'
                       else '其他' end) as tt1;


    ------ITR--------
    --1itr 研发/质量基础data 产品维度
    delete from ads_gp_pg_t_itr_basic_cp_df;
    insert into ads_gp_pg_t_itr_basic_cp_df
    select a.event_date,
           a.cpid,
           a.cptype,
           a.itr_gongdan_num,                  --itr工单数量
           b.values    as itr_gongdan_user_num,--ITR工单客户数量
           c.指标值    as L3_qa_gongdan_num,   --L3软件问题的工单数量
           d.指标值    as L3_bihuan_lv,--L3升单闭环率
           e.指标值    as gongdan_num,--工单数量
           f.hetong_totalnum,                  --合同总数量
           f.hetong_usernum,                   --合同客户数量
           a.gongdanid as id_one,              --工单id列表
           b.gongdanid as id_two,
           c.gongdanid as id_three,
           d.gongdanid as id_four,
           e.gongdanid as id_five
    from (select a.product_id                           as cpid,--产品ID,
                 a.product_type_name                    as cptype,--产品类型,
                 date(a.gmt_create)                     as event_date,--日期,
                 array_to_string(ARRAY_AGG(a.id), ', ') AS gongdanid,-- 工单ID列表,
                 'ITR工单数量'                          as 指标,
                 count(distinct a.id)                   as itr_gongdan_num--指标值
          from ds_itr_workorder_work_order a
          where a.work_order_status <> 0
            and a.gmt_create >= date_trunc('week', current_date - interval '12 week') -- 1 week,1 month,3 month
            and a.gmt_create < date_trunc('week', current_date)
          group by a.product_id, a.product_type_name,
                   date(a.gmt_create)) as a
             left join

         (select a.product_id                           as cpid,--产品ID,
                 a.product_type_name                    as cptype,--产品类型,
                 date(a.gmt_create)                     as event_date,--日期,
                 array_to_string(ARRAY_AGG(a.id), ', ') AS gongdanid,-- 工单ID列表,
                 'ITR工单客户数量'                      as yfkpi,--指标,
                 count(distinct a.product_line_id)      as values--指标值
          from ds_itr_workorder_work_order a
          where a.work_order_status <> 0
            and a.gmt_create >= date_trunc('week', current_date - interval '12 week') -- 1 week,1 month,3 month
            and a.gmt_create < date_trunc('week', current_date)
          group by a.product_id,
                   a.product_type_name,
                   date(a.gmt_create)) as b on a.event_date = b.event_date and a.cpid = b.cpid and a.cptype = b.cptype

             left join
         (select a.product_id                           as cpid,--产品ID,
                 a.product_type_name                    as 产品类型,
                 date(a.gmt_create)                     as 日期,
                 array_to_string(ARRAY_AGG(a.id), ', ') AS gongdanid,-- 工单ID列表,
                 'L3软件问题的工单数量'                 as 指标,
                 count(distinct a.id)                   as 指标值
          from ds_itr_workorder_work_order a
                   inner join ds_itr_workorder_flow_table b on a.id = b.biz_id
                   inner join ds_itr_workorder_flow_node c on b.id = c.flow_id
          where c.node_name = 'L3处理'
            and a.work_order_status <> 0
            and a.problem_big_type = 1554645069768249346
            and a.gmt_create >= date_trunc('week', current_date - interval '12 week') -- 1 week,1 month,3 month
            and a.gmt_create < date_trunc('week', current_date)
          group by a.product_id,
                   a.product_type_name,
                   date(a.gmt_create)) as c on a.event_date = c.日期 and a.cpid = c.cpid and a.cptype = c.产品类型

             left join
         (select a.product_id          as cpid,--产品ID,
                 a.product_type_name   as 产品类型,
                 date(a.gmt_create)    as 日期,
                 array_to_string(ARRAY_AGG(case when c.node_name = 'L3处理' then a.id else null end),
                                 ', ') AS gongdanid,-- 工单ID列表,
                 'L3升单闭环率'        as 指标,
                 ROUND((count(case when c.node_name = 'L3处理' then 1 else null end) /
                        count(distinct a.id) ::NUMERIC),
                       2)              as 指标值
          from ds_itr_workorder_work_order a
                   inner join ds_itr_workorder_flow_table b on a.id = b.biz_id
                   inner join ds_itr_workorder_flow_node c on b.id = c.flow_id
          where a.work_order_status <> 0
            and a.gmt_create >= date_trunc('week', current_date - interval '12 week') -- 1 week,1 month,3 month
            and a.gmt_create < date_trunc('week', current_date)
          group by a.product_id,
                   a.product_type_name,
                   date(a.gmt_create)) as d on a.event_date = d.日期 and a.cpid = d.cpid and a.cptype = d.产品类型
             left join
         (select a.product_id                           as cpid,--产品ID,
                 a.product_type_name                    as 产品类型, --少
                 date(a.gmt_create)                     as 日期,
                 array_to_string(ARRAY_AGG(a.id), ', ') as gongdanid,--新增与遗留缺陷工单ID列表,
                 '工单数量'                             as 指标,
                 count(distinct a.id)                   as 指标值
          from ds_itr_workorder_work_order a
          where a.work_order_status <> 0
            and gmt_create >= date_trunc('week', current_date - interval '12 week') -- 1 week,1 month,3 month
            and gmt_create < date_trunc('week', current_date)
          group by a.product_id,
                   date(a.gmt_create),
                   a.product_type_name) as e on a.event_date = e.日期 and a.cpid = e.cpid and a.cptype = e.产品类型
             left join
         (select t1.日期         event_date,
                 '工号'          usercode,
                 '产品线名称'    cpxname,
                 t1.cpname       cpname,
                 t1.产品实际毛利 cp_maoli,
                 t1.合同产品金额 hetong_gmv,
                 t1.合同客户数   hetong_usernum,
                 t1.合同总数量   hetong_totalnum,
                 t1.回款合同数量 hetong_huinum
          from (select date(field0028)                                                                     日期,
                       --field0010                                                                           工号,
                       field0067                                                                as         cpname,
                       sum(field0046)                                                                      产品实际毛利,
                       sum(case when (field0029 <> '否' or field0078 <> '其他') then field0016 else 0 end) 合同产品金额,
                       count(distinct field0019)                                                           合同客户数,
                       count(distinct field0002)                                                as         合同总数量,
                       count(distinct (case when (field0278) > 0 then field0002 else null end)) as         回款合同数量
                from ds_oa_formmain_105321
                where date(field0028) >= date('2022-01-01')
                  and date(field0028) < date(current_date)
                GROUP BY date(field0028),
                         --field0010,
                         field0067) as t1) as f on a.event_date = f.event_date and a.cptype = f.cpname
    group by a.event_date,
             a.cpid,
             a.cptype,
             a.itr_gongdan_num,
             b.values,--ITR工单客户数量
             c.指标值, --L3软件问题的工单数量
             d.指标值,--L3升单闭环率
             e.指标值,--工单数量
             f.hetong_totalnum, --合同总数量
             f.hetong_usernum, --合同客户数量
             a.gongdanid, --工单id列表
             b.gongdanid,
             c.gongdanid,
             d.gongdanid,
             e.gongdanid;


    --2itr 研发/质量基础data 新增遗留分布
    delete from ads_gp_pg_t_itr_newold_fenbu_df;
    insert into ads_gp_pg_t_itr_newold_fenbu_df
        (select a.product_id                                                                                   as cpid,--产品ID,
                a.product_type_name                                                                            as cptype,--产品类型,
                date(a.gmt_create)                                                                             as event_date,--日期,
                a.product_version                                                                              as banben_name,--版本名称,
                array_to_string(ARRAY_AGG(a.id), ', ')                                                         as pro_id,--反馈项目总数ID列表,
                array_to_string(ARRAY_AGG(case when a.work_order_status not in (0, 5) then a.id else null end),
                                ', ')                                                                          AS wjj_id,--产品内部未解决数工单ID列表,
                array_to_string(ARRAY_AGG(case when a.work_order_status not in (0, 5) then a.id else null end),
                                ', ')                                                                          as yiliu_id,--遗留未解决数工单ID列表,
                array_to_string(ARRAY_AGG(case when a.work_order_status not in (0, 5) then a.id else null end),
                                ', ')                                                                          AS bhlv_id,--闭环率工单ID列表,
                count(a.product_line_id)                                                                       as fankui_pro_qty,--反馈项目总数,
                count(case when a.work_order_status not in (0, 5) then 1 else null end)                        as cp_wjj_qty,--产品内部未解决数,
                count(distinct case
                                   when a.work_order_status not in (0, 5) then product_line_id
                                   else null end)                                                              as yiliu_wjj_qty,--遗留未解决项目数一暂定,
                ROUND((count(case when a.work_order_status not in (0, 5) then 1 else null end) /
                       count(distinct a.id) ::NUMERIC),
                      2)                                                                                       as bihuan_lv--闭环率
         from ds_itr_workorder_work_order a
         where a.work_order_status <> 0
           and a.gmt_create >= date_trunc('week', current_date - interval '12 week') -- 1 week,1 month,3 month
           and a.gmt_create < date_trunc('week', current_date)
         group by a.product_id,
                  a.product_type_name,
                  a.product_version,
                  date(a.gmt_create));


    --3itr 研发/质量基础data 用户问题分布
    delete from ads_gp_pg_t_itr_lastuser_fenbu_df;
    insert into ads_gp_pg_t_itr_lastuser_fenbu_df
        (select a.product_id                           as cpid,--产品ID,
                a.product_type_name                    as cptype,--产品类型,
                date(a.gmt_create)                     as event_date,--日期,
                a.product_version                      as banben_name,--版本名称,
                a.ultimate_customer                    as last_user,--最终客户,
                array_to_string(ARRAY_AGG(a.id), ', ') as gongdanid,--测试工单ID列表,
                count(distinct a.ultimate_customer)    as gongdan_qty--工单数量
         from ds_itr_workorder_work_order a
         where a.work_order_status <> 0
--    and a.ultimate_customer
         group by a.product_id,
                  a.ultimate_customer,
                  a.product_type_name,
                  date(a.gmt_create),
                  a.product_version);

    --4itr 研发/质量基础data 问题类型分布
    delete from ads_gp_pg_t_itr_qtype_fenbu_df;
    insert into ads_gp_pg_t_itr_qtype_fenbu_df
        (select a.product_id                           as cpid,--产品ID,
                a.product_type_name                    as cptype,--产品类型,
                date(a.gmt_create)                     as event_date,--日期,
                a.product_version                      as banben_name,--版本名称,
                b.show_name                            as qa_type,--问题类型,
                array_to_string(ARRAY_AGG(a.id), ', ') as gongdanid,--测试工单ID列表,
                count(b.show_name)                     as cp_qty--产品数量
         from ds_itr_workorder_work_order a
                  inner join ds_itr_workorder_dictionary b on a.problem_small_type = b.id
         where a.work_order_status <> 0
           and a.gmt_create >= date_trunc('week', current_date - interval '12 week') -- 1 week,1 month,3 month
           and a.gmt_create < date_trunc('week', current_date)
         group by a.product_id,
                  a.product_type_name,
                  b.show_name,
                  date(a.gmt_create),
                  a.product_version);

    --5itr 研发/质量基础data 工单等级分布 caiji
    delete from ads_gp_pg_t_itr_gongdan_fenbu_df;
    insert into ads_gp_pg_t_itr_gongdan_fenbu_df
        (select a.product_id                           as cpid,--产品ID,
                a.product_type_name                    as cptype,--产品类型,
                date(a.gmt_create)                     as event_date,--日期,
                a.product_version                      as banben_inf,--版本信息,
                b.show_name                            as gongdan_level,--工单等级,
                array_to_string(ARRAY_AGG(a.id), ', ') as gongdanid,--测试工单ID列表,
                count(b.show_name)                     as gongdan_qty--工单数量
         from ds_itr_workorder_work_order a
                  inner join ds_itr_workorder_dictionary b on a.order_level = b.id
         where a.work_order_status <> 0 --    and
               --    a.gmt_create >= '2022-12-02 00:00:00'
               --    and a.gmt_create <= '2022-12-08 23:00:59'
         group by a.product_id,
                  a.product_type_name,
                  b.show_name,
                  date(a.gmt_create),
                  a.product_version);


    --6itr 研发/质量基础data 产品问题趋势
    delete from ads_gp_pg_t_itr_cp_qushi_df;
    insert into ads_gp_pg_t_itr_cp_qushi_df
        (select a.product_id                           as cpid,--产品ID,
                a.product_type_name                    as cptype,--产品类型,
                date(a.gmt_create)                     as event_date,--日期,
                array_to_string(ARRAY_AGG(a.id), ', ') as cp_new_id,--产品新增问题数工单ID列表,
                array_to_string(ARRAY_AGG(case when a.work_order_status not in (0, 5) then a.id else null end),
                                ', ')                  AS cp_wjj_id,--产品内部未解决数工单ID列表,
                array_to_string(ARRAY_AGG(case when a.work_order_status = 5 then a.id else null end),
                                ', ')                  AS qa_lv_id,--问题闭环率工单ID列表,
                count(distinct a.id)                   as cp_new_qty,--产品新增问题数,
                count(
                        case
                            when a.work_order_status not in (0, 5) then 1
                            else null
                            end
                    )                                  as cp_weijj_qty,--产品内部未解决数,
                ROUND(
                            count(
                                    case
                                        when a.work_order_status = 5 then 1
                                        else null
                                        end
                                ) / count(DISTINCT a.id):: NUMERIC,
                            2
                    )                                  as qa_lv--问题闭环率
         from ds_itr_workorder_work_order a
         where a.work_order_status <> 0
           and a.gmt_create >= date_trunc('week', current_date - interval '12 week') -- 1 week,1 month,3 month
           and a.gmt_create < date_trunc('week', current_date)
         group by a.product_id,
                  date(a.gmt_create),
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
         select a.product_id                           as cpid,--产品ID,
                a.product_type_name                    as cptype,--产品类型,
                a.product_version                      as banben_name,--版本名称
                date(a.gmt_create)                     as event_date,--日期,
                --a.product_line_id                 as 合同号一需要匹配行业,
                c.field0380                            as first_cate,--第一行业,
                c.field0381                            as second_cate,--第二行业,
                array_to_string(ARRAY_AGG(a.id), ', ') AS gongdanid,--合同分布工单ID列表,
                count(distinct a.product_line_id)      as gongdan_qty--工单数量
         from ds_itr_workorder_work_order a
                  inner join formson_table b on product_line_id = cast(b.id as varchar(64))
                  inner join ds_oa_formmain_1119 c on b.formmain_id = c.id
         where a.work_order_status <> 0
--   and a.gmt_create >= date_trunc('month', current_date - interval '1 month') -- 1 week,1 month,3 month
--   and a.gmt_create < date_trunc('month', current_date)
         group by a.product_id,
                  a.product_type_name,
                  a.product_version,
                  --a.product_line_id,
                  date(a.gmt_create),
                  c.field0380,
                  c.field0381);


    --8 itr工单明细
    delete from ads_gp_pg_t_itr_gongdan_detail_df;
    insert into ads_gp_pg_t_itr_gongdan_detail_df
    select a.id                as id,--工单ID,
           a.order_code        as id_code,--工单编号,
           a.order_title       as gongdan_name,--工单标题,
           a.product_id        as cpid,--产品ID,
           a.product_type_name as cptype,--产品类型,
           a.product_version   as cpbanben,--产品版本,
           b.show_name         as qa_big,--问题大类,
           c.show_name         as aq_small,--问题小类,
           d.show_name         as gongdan_level--工单等级
    from ds_itr_workorder_work_order a
             inner join ds_itr_workorder_dictionary b on a.problem_big_type = b.id
             inner join ds_itr_workorder_dictionary c on a.problem_small_type = c.id
             inner join ds_itr_workorder_dictionary d on a.order_level = d.id
    where a.work_order_status <> 0
      and a.gmt_create >= date_trunc('month', current_date - interval '3 month') -- 1 week,1 month,3 month
      and a.gmt_create < date_trunc('month', current_date);

    --9 itr挂起未响应订单
    delete from ads_gp_pg_t_itr_none_down_df;
    insert into ads_gp_pg_t_itr_none_down_df
    select Date(a.gmt_create)                                         event_date,--as 日期,
           --写表的时候，这里要加两个字段,客户投诉数量,客户投诉详情
           a.product_id                                               cpid,--as 产品ID,
           a.product_type_name                                        cptype,--as 产品类型,
           a.product_version                                          banben_inf,--as 版本信息,
           count(1)                                                   none_num,--as 未响应工单量,
           array_to_string(ARRAY_AGG(a.id), ', ')                     none_gongdanid,--as 未响应工单量ID列表, --增加工单ID，用于产品下钻
           count(case when b.hang_status = 0 then 1 else null end) as down_num,--挂起工单数,
           array_to_string(ARRAY_AGG(case when b.hang_status = 0 then a.id else null end),
                           ', ')                                   as down_gongdanid,--挂起工单数ID列表,    --增加工单ID，用于产品下钻
           null                                                    as none_one,
           null                                                    as none_two
    from ds_itr_workorder_work_order a
             inner join ds_itr_workorder_hang_up b on a.id = b.order_id
    where a.work_order_status <> 0
    group by a.product_id,
             --增加产品ID，兼容后续
             a.product_version,
             a.product_type_name,
             Date(a.gmt_create);


end
$$
    language 'plpgsql';


--每天4点执行一次
create or replace function proc2() returns trigger as
$$
begin
    --⑨2023 经营结果数据表
    --delete  select * from ads_gp_all_t_cpx_cp_zhibiao_df where date(dateid)=date('2023-03-30');
    insert into ads_gp_all_t_cpx_cp_zhibiao_df
        (select usercode                                                                               as dateid, --存储为日期分区
                cpxname,
                cpname,
                unnest(array ['产品实际毛利','合同产品金额','合同总数量','回款合同数量','订单成功率']) as yfkpi,--指标,
                unnest(array [产品实际毛利,合同产品金额,合同总数量,回款合同数量,订单成功率])           as values--指标值
         from (select (current_date - interval '1 days')           usercode,
                      cpxname,
                      cpname,
                      sum(cp_maoli) / 1                         as 产品实际毛利,
                      sum(hetong_gmv) / 1                       as 合同产品金额,
                      sum(hetong_totalnum) / 1                  as 合同总数量,
                      sum(hetong_huinum) / 1                    as 回款合同数量,
                      sum(hetong_huinum) / sum(hetong_totalnum) as 订单成功率
               from ads_gp_oa_t_hangye_hetong_sales_df
               where date_part('year', event_date) = '2023'
               group by --usercode,
                        cpxname,
                        cpname) as tt
         union all
         select (current_date - interval '1 days') usercode,
                cpxname,
                cpname,
                '商机预计成交金额' as              指标,
                sum(yuji_gmv)      as              指标值
         from ads_gp_oa_t_hangye_shangji_sales_df
         where date_part('year', event_date) = '2023'
         group by --usercode,
                  cpxname,
                  cpname
         union all
         select (current_date - interval '1 days') usercode,
                '产品线名称'                       cpxname,
                t1.cpname,
                '产品实际收入'  as                 指标,
                t1.产品实际收入 as                 指标值
         from (select field0067 as   cpname,
                      sum(field0278) 产品实际收入
               from ds_oa_formmain_105321
               GROUP BY field0067) as t1);
end
$$
    language 'plpgsql';







