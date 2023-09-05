select item_code, count(*) from ods_oa_finance_cp_map_relation
    group by item_code
having  count(*)>=2
--①产线员工数量
-- delete  from ads_gp_oa_t_hr_eminf_inf_df where date(dateid)=date('2023-06-19')
--select *  from ads_gp_oa_t_hr_eminf_inf_df where date(dateid)=date('2023-06-20')
insert into ads_gp_oa_t_hr_eminf_inf_df
select tt1.cpxname                            as depart_cpx,
       count(distinct field0002)              as yuangong_num,
       date(current_date - interval '1 days') as dateid,
       null                                   as none_two
from (select case when t2.二级部门 = '智能检测与终端产品线' then '智能检测与终端产品线' else t2.二级部门 end as cpxname,
             t1.field0002,
             t1.field0006                                                                                em_name,
             t2.二级部门
      from ex_ods_oa_abv5_formmain_10037 as t1
               join ex_ods_oa_abv5_depart_level_all as t2 on t1.field0200 = cast(t2."部门ID" as text)
      where field0180 = '-7808949530931608431'
        and t2.二级部门 in ('智能检测与终端产品线', '基础安全产品线', 'AiLPHA态势感知产品线', '日志与流量产品部','数据安全产品线','云产品线')
      group by case when t2.二级部门 = '智能检测与终端产品线' then '智能检测与终端产品线' else t2.二级部门 end, t1.field0002,
               t1.field0006, t2.二级部门) as tt1
group by tt1.cpxname;


--②行业维度 销售及用户数相关数据
truncate table ads_gp_oa_t_hangye_hetong_sales_df;
insert into  ads_gp_oa_t_hangye_hetong_sales_df
select t1.日期         event_date,
       '工号'          usercode,
       cpx_name        cpxname,
       t1.cpname       cpname,
       t1.一级行业     cate_one,
       t1.二级行业     cate_two,
       t1.三级行业     cate_three,
       t1.产品实际毛利 cp_maoli,
       t1.合同产品金额 hetong_gmv,
       t1.合同客户数   hetong_usernum,
       t1.合同总数量   hetong_totalnum,
       t1.回款合同数量 hetong_huinum,
       cp_class,
       t1.出货量 as    purchase_qty
from (select date(a.field0028)                                                                   日期,
             --field0010                                                                           工号,
             a.field0091                                                                         一级行业,
             a.field0092                                                                         二级行业,
             a.field0275                                                                         三级行业,
             a.field0111                                                                as         cpname,
             a.field0065 cpx_name,
             null cp_class,
             --cp_spc,
             sum(case when field0066 = '产品' then field0103 else 0 end)                         出货量,
             sum(a.field0046)                                                                    产品实际毛利,
             sum(case when (field0029 <> '否' or field0078 <> '其他') then field0016 else 0 end) 合同产品金额,
             count(distinct field0019)                                                           合同客户数,
             count(distinct field0002)                                                as         合同总数量,
             count(distinct (case when (field0278) > 0 then field0002 else null end)) as         回款合同数量
      from ex_ods_oa_abv5_formmain_105321 as a
               left join ods_oa_finance_cp_map_relation as b on a.field0004 = b.item_code
      where date(field0028) >= date('2022-01-01')
        and date(field0028) < date(current_date)
      GROUP BY date(field0028),
               --field0010,
               field0091,
               field0092,
               field0275, a.field0111 , a.field0065) as t1;


--③办事处维度 销售及用户数相关数据
truncate table ads_gp_oa_t_banshichu_hetong_sales_df;
insert into ads_gp_oa_t_banshichu_hetong_sales_df
select tt1.日期         event_date,
       '工号'           usercode,
       cpx_name         cpxname,
       tt1.cpname       cpname,
       tt1.办事处ID     banshichu_id,
       tt1.办事处名称   banshichu_name,
       tt1.产品实际毛利 cp_maoli,
       tt1.合同产品金额 hetong_cp_gmv,
       tt1.合同客户数   hetong_usernum,
       tt1.合同总数量   hetong_totalnum,
       tt1.回款合同数量 hetong_huinum,
       tt1.cp_class
from (select date(t1.field0028)                                                                  日期,
             --t1.field0010                                                                        工号,
             t1.field0183                                                                        办事处ID,
             t2."NAME"                                                                           办事处名称,
             field0111                                                                      as   cpname,
             field0065 cpx_name,
             null as cp_class,
             sum(t1.field0046)                                                                   产品实际毛利,
             sum(case when (field0029 <> '否' or field0078 <> '其他') then field0016 else 0 end) 合同产品金额,
             count(distinct t1.field0019)                                                        合同客户数,
             count(distinct field0002)                                                      as   合同总数量,
             count(distinct (case when (t1.field0278) > 0 then t1.field0002 else null end)) as   回款合同数量
      from ex_ods_oa_abv5_formmain_105321 as t1
               left join ex_ods_oa_abv5_org_unit as t2 on t1.field0183 = cast(t2."ID" as text)
               left join ods_oa_finance_cp_map_relation as b on t1.field0004 = b.item_code
      where date(field0028) >= date('2022-01-01')
        and date(field0028) < date(current_date)
      GROUP BY date(t1.field0028),
               --t1.field0010,
               t1.field0183,
               t2."NAME", field0111 , field0065) as tt1;

--④行业维度 各层级商机销售
delete
from ads_gp_oa_t_hangye_shangji_sales_df;
insert into ads_gp_oa_t_hangye_shangji_sales_df
select tt1.日期         event_date,
       win_lv           usercode,
       tt1.产品id       cpxname,
       tt1.产品名称     cpname,
       tt1.一级行业名称 cate_one,
       tt1.二级行业名称 cate_two,
       tt1.三级行业名称 cate_three,
       tt1.状态         pro_status,
       tt1.预计成交金额 yuji_gmv,
       tt1.cp_class
from (select date(t1.field0018)           日期,
             --t1.field0281           工号,
             --t1.field0160         一级行业,
             t3."SHOWVALUE"                 一级行业名称,
             --t1.field0161         二级行业,
             t4."SHOWVALUE"                 二级行业名称,
             --t1.field0282         三级行业,
             t5."SHOWVALUE"                 三级行业名称,
             t6.cpx_name                 产品id,
             t6.cp_name                 产品名称,
             t6.cp_class,
             t1.field0017              as win_lv,--赢单率
             case
                 when t1.field0028 = '5146369105019027477' then '暂停'
                 when t1.field0028 = '2945291516390491342' then '丢单'
                 when t1.field0028 = '-5091227863388631683' then '失效'
                 when t1.field0028 = '-2036947102919343196' then '已签合同'
                 when t1.field0028 = '-4552238618822292475' then '进行中'
                 else '其他' end       as 状态,
             sum(t2.field0046) * 10000 as 预计成交金额
      from ex_ods_oa_abv5_formmain_2156 t1
               left join ex_ods_oa_abv5_formson_2157 t2 on t1."ID" = t2.formmain_id
               left join ex_ods_oa_abv5_ctp_enum_item t3 on cast(t1.field0160 as text) = cast(t3."ID" as text)
               left join ex_ods_oa_abv5_ctp_enum_item t4 on cast(t1.field0161 as text) = cast(t4."ID" as text)
               left join ex_ods_oa_abv5_ctp_enum_item t5 on cast(t1.field0282 as text) = cast(t5."ID" as text)
               left join ods_oa_finance_cp_map_relation t6 on t2.field0097 = t6.item_code
      where date(t1.field0018) >= date('2022-01-01')
        and t1.field0286 is null
      GROUP BY date(t1.field0018),
               --t1.field0281,
               --t1.field0160
               t3."SHOWVALUE",
               --t1.field0161,
               t4."SHOWVALUE",
               --t1.field0282,
               t5."SHOWVALUE", t6.cpx_name,
               t6.cp_name,t6.cp_class, t1.field0017,
               case
                   when t1.field0028 = '5146369105019027477' then '暂停'
                   when t1.field0028 = '2945291516390491342' then '丢单'
                   when t1.field0028 = '-5091227863388631683' then '失效'
                   when t1.field0028 = '-2036947102919343196' then '已签合同'
                   when t1.field0028 = '-4552238618822292475' then '进行中'
                   else '其他' end) as tt1;

--⑤办事处维度 各层级商机销售
delete
from ads_gp_oa_t_banshichu_shangji_sales_df;
insert into ads_gp_oa_t_banshichu_shangji_sales_df
select tt1.日期         event_date,
       win_lv           usercode,
       tt1.产品id       cpxname,
       tt1.产品名称     cpname,
       tt1.办事处ID     banshichu_id,
       tt1.办事处名称   banshichu_name,
       tt1.状态         pro_status,
       tt1.预计成交金额 yuji_gmv,
       tt1.cp_class
from (select date(t1.field0018)           日期,
             --t1.field0281           工号,
             t1.field0040                 办事处ID,
             t3."NAME"                    办事处名称,
             t4.cpx_name                  产品id,
             t4.cp_name                   产品名称,
             t4.cp_class,
             t1.field0017              as win_lv,--赢单率
             case
                 when t1.field0028 = '5146369105019027477' then '暂停'
                 when t1.field0028 = '2945291516390491342' then '丢单'
                 when t1.field0028 = '-5091227863388631683' then '失效'
                 when t1.field0028 = '-2036947102919343196' then '已签合同'
                 when t1.field0028 = '-4552238618822292475' then '进行中'
                 else '其他' end       as 状态,
             sum(t2.field0046) * 10000 as 预计成交金额
      from ex_ods_oa_abv5_formmain_2156 t1
               left join ex_ods_oa_abv5_formson_2157 t2 on t1."ID" = t2.formmain_id
               left join ex_ods_oa_abv5_org_unit as t3 on t1.field0040 = cast(t3."ID" as text)
               left join ods_oa_finance_cp_map_relation t4 on t2.field0097 = t4.item_code
      where date(t1.field0018) >= date('2022-01-01')
      GROUP BY date(t1.field0018),
               --t1.field0281,
               t1.field0040,
               t3."NAME",
               t4.cpx_name,
               t4.cp_name,
               t4.cp_class,
               t1.field0017,
               case
                   when t1.field0028 = '5146369105019027477' then '暂停'
                   when t1.field0028 = '2945291516390491342' then '丢单'
                   when t1.field0028 = '-5091227863388631683' then '失效'
                   when t1.field0028 = '-2036947102919343196' then '已签合同'
                   when t1.field0028 = '-4552238618822292475' then '进行中'
                   else '其他' end) as tt1;


--⑥测试合同数据
delete
from ads_gp_oa_t_ceshi_hetong_qty_df;
insert into create_table ads_gp_oa_t_ceshi_hetong_qty_df as
select tt1.日期         event_date,
       '工号'           usercode,
       '产品线名称'     cpxname,
       tt1.cpname       cpname,
       tt1.测试合同数量 hetong_ceshi_qty
from (select 日期, cpname, count(distinct id) as 测试合同数量
      from (SELECT t1."ID" as id,
                   date(t1.start_date) as 日期,
                   t2.cpname
            FROM ex_ods_oa_abv5_formmain_18955 AS t1
                     LEFT JOIN
                 (SELECT formmain_id,
                         field0060 as 料品编码,
                         field0051 as cpname
                  FROM ex_ods_oa_abv5_formson_18956
                  where field0051 is not null
                  UNION ALL
                  SELECT formmain_id, field0234 as 料品编码, field0071
                  FROM ex_ods_oa_abv5_formson_19389
                  where field0071 is not null) AS t2 ON t1."ID" = t2.formmain_id
            where date(t1.start_date) >= date('2022-01-01')
              and date(t1.start_date) < date(current_date)) as tt
      group by 日期, cpname) tt1;

--⑦RM评审数据
delete
from ads_gp_rm_t_pingsheng_hetong_qty_df;
insert into ads_gp_rm_t_pingsheng_hetong_qty_df as
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
      from ex_ods_pass_ecology_uf_IPDxqgl as t1
               left join ex_ods_pass_ecology_uf_productline as t2 on t1.cpx = t2.cpxid
               left join ex_ods_pass_ecology_uf_product as t3 on t1.cpxcpbd = t3.cpid
      where date(t1.tjsj) >= date('2022-01-01')
        and date(t1.tjsj) < date(current_date)
      group by regexp_split_to_table(concat_ws(',', t1.tjr, t1.cpjl, t1.reviewer), ','),
               t1.tjsj, t1.id, t1.xqzt, t1.cpx, t1.cpxcpbd, t2.cpxmc, t3.name) as tt
group by tjsj, 产品线名称, 产品名称;

--⑧员工产品线 产品宽表；权限使用
--select * from ads_gp_rm_t_hr_cp_inf_df
delete
from ads_gp_rm_t_hr_cp_inf_df;
insert into ads_gp_rm_t_hr_cp_inf_df
(select 员工工号          usercode,
        em_name,--员工姓名
        t2.需求表产品线ID cpxid,
        t2.需求表产品ID   cpid,
        t2.产品名称       cpname,
        t2.产品线名称     cpxname,
        t2.cp_class
 from (select 员工工号,
              em_name,
              id
       from (select productline as                         id,
                    regexp_split_to_table(scmanager, ',') 员工系统自带ID
      from ex_ods_pass_ecology_matrixtable_6
             union all
             select productline,
                    regexp_split_to_table(jfmanager, ',')
             from ex_ods_pass_ecology_matrixtable_6
             union all
             select productline,
                    regexp_split_to_table(yfmanager, ',')
             from ex_ods_pass_ecology_matrixtable_6
             union all
             select productline,
                    regexp_split_to_table(csmanager, ',')
             from ex_ods_pass_ecology_matrixtable_6
             union all
             select productline,
                    regexp_split_to_table(yfcmanager, ',')
             from ex_ods_pass_ecology_matrixtable_6
             union all
             select productline,
                    regexp_split_to_table(productinspector, ',')
             from ex_ods_pass_ecology_matrixtable_6
             union all
             select productline,
                    regexp_split_to_table(zlfzr, ',')
             from ex_ods_pass_ecology_matrixtable_6
             union all
             select productline,
                    regexp_split_to_table(yfmanmager, ',')
             from ex_ods_pass_ecology_matrixtable_6) as t1
                left join
            (SELECT a.id        员工系统自带ID,
                    a.loginid   员工工号,
                    b.field0006 em_name
             FROM ex_ods_pass_ecology_hrmresource as a
                      left join ex_ods_oa_abv5_formmain_10037 as b on a.loginid = b.field0002
             GROUP BY a.id,
                      a.loginid, b.field0006) as t2
            on cast(t1.员工系统自带ID as text) = cast(t2.员工系统自带ID as text)
       group by 员工工号, em_name, id) as t1
          left join
      (select t1.cpxid    需求表产品线ID,
              t1.cpx_name 产品线名称,
              null cp_class,
              'null' as   需求表产品ID,
              t2.financial_pro_class_name  产品名称
       from (select cpxid
                  , cpxmc
                  , case when cpxmc = '智能检测与终端产品线' then '智能检测与终端产品线' else cpxmc end cpx_name
             from ex_ods_pass_ecology_uf_productline
             group by cpxid
                    , cpxmc
                    , case when cpxmc = '智能检测与终端产品线' then '智能检测与终端产品线' else cpxmc end) as t1
                left join ods_finance_map_relation as t2 on t1.cpx_name = t2.financial_pro_line_name) as t2
      on t1.id = t2.需求表产品线ID
 where 员工工号 is not null  and
       产品线名称 in ('基础安全产品线','智能检测与终端产品线','AiLPHA态势感知产品线', '日志与流量产品部','数据安全产品线','云产品线')
 group by 员工工号,
          em_name,
          t2.需求表产品线ID,
          t2.需求表产品ID,
          t2.产品名称,
          t2.产品线名称,
          t2.cp_class);

select * from ex_ods_pass_ecology_matrixtable_6
--⑨商机项目预测状态数据
delete
from ads_oa_t_shangji_status_sales_df;
insert into ads_oa_t_shangji_status_sales_df
select tt1.日期         event_date,
       tt1.产品id       cpid,
       tt1.产品名称     cpname,
       tt1.pro_tag,--项目预测
       tt1.预计成交金额 yuji_gmv,
       cp_class          cp_class,
       null             none_two,
       null             none_three
from (select date(t1.field0018)           日期,
             t6.cpx_name                 产品id,
             t6.cp_class,
             t6.cp_name                 产品名称,
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
      from ex_ods_oa_abv5_formmain_2156 t1
               left join ex_ods_oa_abv5_formson_2157 t2 on t1."ID" = t2.formmain_id
               left join ods_oa_finance_cp_map_relation t6 on t2.field0097 = t6.item_code
      where date(t1.field0018) >= date('2022-01-01')
        and t1.field0286 is null
      GROUP BY date(t1.field0018),
               t6.cpx_name,
               t6.cp_class,
               t6.cp_name,
               case
                   when t1.field0014 in ('5937608534944795120') then '输单/暂停'
                   when t1.field0014 in ('-913553023266763785', '-8233068084558609166') then '争取'
                   when t1.field0014 in ('1841219522854799953') then '中标'
                   when t1.field0014 in ('-8517000066082717530', '8209823446584085971') then '商机'
                   when t1.field0014 in ('1420566586594969807') then '承诺'
                   when t1.field0014 in ('-6786166406896401062', '8207352573662219079') then '信息'
                   when t1.field0014 in ('-7497924421957762614') then '签订合同'
                   else '其他' end) as tt1;

--10：销售人员对应产品销售数据
delete
from ads_oa_t_sales_em_sales_df;
insert into ads_oa_t_sales_em_sales_df
select t1.日期 as      event_date,
       t1.工号         usercode,
       t2.em_name,
       t1.cpname,
       t1.产品实际毛利 cp_maoli,
       t1.合同产品金额 hetong_gmv,
       t1.合同客户数   hetong_usernum,
       t1.合同总数量   hetong_totalnum,
       t1.回款合同数量 hetong_huinum,
       working_status  working_status,
       cpx_name        cpx_name,
       cp_class            cp_class
from (select date(a.field0028)                                                                         日期,
             a.field0010                                                                               工号,
             b.cp_name                                                                    as           cpname,
             b.cpx_name,
             b.cp_class,
             sum(a.field0046)                                                                          产品实际毛利,
             sum(case when (a.field0029 <> '否' or a.field0078 <> '其他') then a.field0016 else 0 end) 合同产品金额,
             count(distinct a.field0019)                                                               合同客户数,
             count(distinct a.field0002)                                                  as           合同总数量,
             count(distinct (case when (a.field0278) > 0 then a.field0002 else null end)) as           回款合同数量
      from ex_ods_oa_abv5_formmain_105321 as a
               left join ods_oa_finance_cp_map_relation as b on a.field0004 = b.item_code
      where date(a.field0028) >= date('2022-01-01')
        and date(a.field0028) < date(current_date)
      GROUP BY date(a.field0028),
               a.field0010, b.cp_name,
               b.cpx_name,b.cp_class) as t1
         left join
     (SELECT a.id                                                                       员工系统自带ID,
             a.loginid                                                                  员工工号,
             b.field0006                                                                em_name,
             case when b.field0180 = '-7808949530931608431' then '在职' else '离职' end working_status
      FROM ex_ods_pass_ecology_hrmresource as a
               left join ex_ods_oa_abv5_formmain_10037 as b on a.loginid = b.field0002
      GROUP BY a.id,
               a.loginid, b.field0006, case when b.field0180 = '-7808949530931608431' then '在职' else '离职' end) as t2
     on t1.工号 = t2.员工工号;


--11：商机合同数
delete
from ads_gp_oa_t_hetong_shangji_qty_df;
insert into  ads_gp_oa_t_hetong_shangji_qty_df
select *,
       null as none_one,
       null as none_two,
       null as none_three
from (select substr(to_char(t1.field0018, 'YYYY-MM--DD'), 1, 7)                        event_date,--         日期,
             coalesce(t6.cpx_name, 'null')                                             cpid,--       产品id,
             coalesce(t6.cp_name, 'null')                                              cpname,--      产品名称,
             '非赢单率'                                                             as win_lv,
             sum(case when t1.field0028 = '-2036947102919343196' then 1 else 0 end) as hetong_num,
             sum(t2.field0046) * 10000                                                 yuji_gmv--         as 预计成交金额
      from ex_ods_oa_abv5_formmain_2156 t1
               left join ex_ods_oa_abv5_formson_2157 t2 on t1."ID" = t2.formmain_id
               left join ods_oa_finance_cp_map_relation t6 on t2.field0097 = t6.item_code
      where date(t1.field0018) >= date('2022-01-01')
        and t1.field0286 is null
        and t2.field0097 is not null
        and t6.item_code is not null
      group by grouping sets ((substr(to_char(t1.field0018, 'YYYY-MM--DD'), 1, 7), t6.cpx_name,
                               t6.cp_name))

      union all

      select substr(to_char(t1.field0018, 'YYYY-MM--DD'), 1, 7)                        event_date,--         日期,
             coalesce(t6.cpx_name, 'null')                                             cpid,--       产品id,
             'total'                                                                   cpname,--      产品名称,
             '非赢单率'                                                             as win_lv,
             sum(case when t1.field0028 = '-2036947102919343196' then 1 else 0 end) as hetong_num,
             sum(t2.field0046) * 10000                                                 yuji_gmv--         as 预计成交金额
      from ex_ods_oa_abv5_formmain_2156 t1
               left join ex_ods_oa_abv5_formson_2157 t2 on t1."ID" = t2.formmain_id
               left join ods_oa_finance_cp_map_relation t6 on t2.field0097 = t6.item_code
      where date(t1.field0018) >= date('2022-01-01')
        and t1.field0286 is null
        and t2.field0097 is not null
        and t6.item_code is not null
        and cpx_name in ('智能检测与终端产品线', '基础安全产品线', 'AiLPHA大数据智能安全产品线', '云产品线')
      group by grouping sets ((substr(to_char(t1.field0018, 'YYYY-MM--DD'), 1, 7), t6.cpx_name))

      union all
      select substr(to_char(t1.field0018, 'YYYY-MM--DD'), 1, 7)                        日期,
             'total'                                                                as 产品id,
             'total'                                                                as 产品名称,
             '非赢单率'                                                             as win_lv,
             sum(case when t1.field0028 = '-2036947102919343196' then 1 else 0 end) as hetong_num,
             sum(t2.field0046) * 10000                                              as 预计成交金额
      from ex_ods_oa_abv5_formmain_2156 t1
               left join ex_ods_oa_abv5_formson_2157 t2 on t1."ID" = t2.formmain_id
               left join ods_oa_finance_cp_map_relation t6 on t2.field0097 = t6.item_code
      where date(t1.field0018) >= date('2022-01-01')
        and t1.field0286 is null
        and t2.field0097 is not null
        and t6.item_code is not null
      group by substr(to_char(t1.field0018, 'YYYY-MM--DD'), 1, 7)

      union all

      select substr(to_char(t1.field0018, 'YYYY-MM--DD'), 1, 4)                        日期,
             coalesce(t6.cpx_name, 'null')                                             产品id,
             coalesce(t6.cp_name, 'null')                                              产品名称,
             cast(t1.field0017 as text)                                             as win_lv,--赢单率
             sum(case when t1.field0028 = '-2036947102919343196' then 1 else 0 end) as hetong_num,
             sum(t2.field0046) * 10000                                              as 预计成交金额
      from ex_ods_oa_abv5_formmain_2156 t1
               left join ex_ods_oa_abv5_formson_2157 t2 on t1."ID" = t2.formmain_id
               left join ods_oa_finance_cp_map_relation t6 on t2.field0097 = t6.item_code
      where date(t1.field0018) >= date('2022-01-01')
        and t1.field0286 is null
        and t2.field0097 is not null
        and t6.item_code is not null
      GROUP BY grouping sets ((substr(to_char(t1.field0018, 'YYYY-MM--DD'), 1, 4), t6.cpx_name,
                               t6.cp_name, cast(t1.field0017 as text)))


      union all

      select substr(to_char(t1.field0018, 'YYYY-MM--DD'), 1, 4)                        日期,
             coalesce(t6.cpx_name, 'null')                                             产品id,
             'total'                                                                   产品名称,
             cast(t1.field0017 as text)                                             as win_lv,--赢单率
             sum(case when t1.field0028 = '-2036947102919343196' then 1 else 0 end) as hetong_num,
             sum(t2.field0046) * 10000                                              as 预计成交金额
      from ex_ods_oa_abv5_formmain_2156 t1
               left join ex_ods_oa_abv5_formson_2157 t2 on t1."ID" = t2.formmain_id
               left join ods_oa_finance_cp_map_relation t6 on t2.field0097 = t6.item_code
      where date(t1.field0018) >= date('2022-01-01')
        and t1.field0286 is null
        and t2.field0097 is not null
        and t6.item_code is not null
        and cpx_name in ('智能检测与终端产品线', '基础安全产品线', 'AiLPHA大数据智能安全产品线', '云产品线')
      GROUP BY grouping sets ((substr(to_char(t1.field0018, 'YYYY-MM--DD'), 1, 4), t6.cpx_name,
                               cast(t1.field0017 as text)))

      union all

      select substr(to_char(t1.field0018, 'YYYY-MM--DD'), 1, 4)                        日期,
             'total'                                                                as 产品id,
             'total'                                                                as 产品名称,
             cast(t1.field0017 as text)                                             as win_lv,--赢单率
             sum(case when t1.field0028 = '-2036947102919343196' then 1 else 0 end) as hetong_num,
             sum(t2.field0046) * 10000                                              as 预计成交金额
      from ex_ods_oa_abv5_formmain_2156 t1
               left join ex_ods_oa_abv5_formson_2157 t2 on t1."ID" = t2.formmain_id
               left join ods_oa_finance_cp_map_relation t6 on t2.field0097 = t6.item_code
      where date(t1.field0018) >= date('2022-01-01')
        and t1.field0286 is null
        and t2.field0097 is not null
        and t6.item_code is not null
      GROUP BY substr(to_char(t1.field0018, 'YYYY-MM--DD'), 1, 4),
               t1.field0017) as t;


--12：商机项目预测值明细
delete from ads_gp_oa_t_shangji_project_sales_df;
insert into ads_gp_oa_t_shangji_project_sales_df
select cast(md5(COALESCE(to_char(日期, 'YYYY-MM-DD'), 'fajonfjfadoijfadfjiefjngj4344453432gfsdg')) as text) ||
       cast(md5(COALESCE(商机名称, 'fajonfjfadoijfadfjiefjngj4344453432gfsdg')) as text) ||
       cast(md5(COALESCE(客户名称, 'fajonfjfadoijfadfjiefjngj4344453432gfsdg')) as text) ||
       cast(md5(COALESCE(cast(win_lv as text), 'fajonfjfadoijfadfjiefjngj4344453432gfsdg')) as text) ||
       cast(md5(COALESCE(产品id, 'fajonfjfadoijfadfjiefjngj4344453432gfsdg')) as text) ||
       cast(md5(COALESCE(产品名称, 'fajonfjfadoijfadfjiefjngj4344453432gfsdg')) as text) as uni_id,
       tt1.日期                                                                             event_date,
       商机名称                                                                          as business_name,
       客户名称                                                                             user_name,
       win_lv                                                                               win_lv,
       tt1.产品id                                                                           cpxname,
       tt1.cp_class,
       tt1.产品名称                                                                         cpname,
       tt1.预计成交金额                                                                     yuji_gmv
from (select date(t1.field0018)           日期,
             field0002                    商机名称,
             field0004                    客户名称,
             t6.cpx_name                  产品id,
             t6.cp_class,
             t6.cp_name                   产品名称,
             t1.field0017              as win_lv,--赢单率
             sum(t2.field0046) * 10000 as 预计成交金额
      from ex_ods_oa_abv5_formmain_2156 t1
               left join ex_ods_oa_abv5_formson_2157 t2 on t1."ID" = t2.formmain_id
               left join ods_oa_finance_cp_map_relation t6 on t2.field0097 = t6.item_code
      where date(t1.field0018) >= date('2022-01-01')
        and t1.field0286 is null
      GROUP BY date(t1.field0018),
               field0002,
               field0004, t6.cpx_name, t6.cp_class,
               t6.cp_name, t1.field0017) as tt1;


--13 汇总-产品维度 合同数
delete from ads_gp_oa_t_cp_hetong_qty_df;
insert into  ads_gp_oa_t_cp_hetong_qty_df
select t1.日期         event_date,
       '工号'          usercode,
       cpx_name        cpxname,
       cp_class,
       t1.cpname       cpname,
       t1.产品实际毛利 cp_maoli,
       t1.合同产品金额 hetong_gmv,
       t1.合同客户数   hetong_usernum,
       t1.合同总数量   hetong_totalnum,
       t1.回款合同数量 hetong_huinum,
       t1.出货量 as    purchase_qty
from (select substr(to_char(a.field0028, 'YYYY-MM-DD'), 1, 4)                                    日期,
             --field0010                                                                           工号,
             b.cp_name                                                                as         cpname,
             b.cpx_name,
             b.cp_class,
             --cp_spc,
             sum(case when field0066 = '产品' then field0103 else 0 end)                         出货量,
             sum(a.field0046)                                                                    产品实际毛利,
             sum(case when (field0029 <> '否' or field0078 <> '其他') then field0016 else 0 end) 合同产品金额,
             count(distinct field0019)                                                           合同客户数,
             count(distinct field0002)                                                as         合同总数量,
             count(distinct (case when (field0278) > 0 then field0002 else null end)) as         回款合同数量
      from ex_ods_oa_abv5_formmain_105321 as a
               left join ods_oa_finance_cp_map_relation as b on a.field0004 = b.item_code
      where date(field0028) >= date('2022-01-01')
        and date(field0028) < date(current_date)
      GROUP BY substr(to_char(a.field0028, 'YYYY-MM-DD'), 1, 4),
               --field0010,
               b.cp_name, b.cpx_name, b.cp_class) as t1;

--14 汇总-型号维度 合同数
delete from ads_gp_oa_t_spc_hetong_qty_df;
insert into ads_gp_oa_t_spc_hetong_qty_df
select t1.日期         event_date,
       '工号'          usercode,
       cpx_name        cpxname,
       cp_class,
       t1.cpname       cpname,
       cp_spc,
       t1.产品实际毛利 cp_maoli,
       t1.合同产品金额 hetong_gmv,
       t1.合同客户数   hetong_usernum,
       t1.合同总数量   hetong_totalnum,
       t1.回款合同数量 hetong_huinum,
       t1.出货量 as    purchase_qty
from (select substr(to_char(a.field0028, 'YYYY-MM-DD'), 1, 4)                                    日期,
             --field0010                                                                           工号,
             b.cp_name                                                                as         cpname,
             b.cpx_name,
             b.cp_class,
             cp_spc,
             sum(case when field0066 = '产品' then field0103 else 0 end)                         出货量,
             sum(a.field0046)                                                                    产品实际毛利,
             sum(case when (field0029 <> '否' or field0078 <> '其他') then field0016 else 0 end) 合同产品金额,
             count(distinct field0019)                                                           合同客户数,
             count(distinct field0002)                                                as         合同总数量,
             count(distinct (case when (field0278) > 0 then field0002 else null end)) as         回款合同数量
      from ex_ods_oa_abv5_formmain_105321 as a
               left join ods_oa_finance_cp_map_relation as b on a.field0004 = b.item_code
      where date(field0028) >= date('2022-01-01')
        and date(field0028) < date(current_date)
      GROUP BY substr(to_char(a.field0028, 'YYYY-MM-DD'), 1, 4),
               --field0010,
               b.cp_name, b.cpx_name, b.cp_class, cp_spc) as t1;

------ITR--------
--1itr 研发/质量基础data 产品维度
delete
from ads_gp_pg_t_itr_basic_cp_df;
insert into ads_gp_pg_t_itr_basic_cp_df
select a.event_date,
       a.cpid,
       a.cptype,
       a.itr_gongdan_num,                  --itr工单数量
       b.values    as itr_gongdan_user_num,--ITR工单客户数量
       c.指标值    as L3_qa_gongdan_num,   --L3软件问题的工单数量
       d.指标值    as L3_bihuan_lv,--L3升单个数
       e.指标值    as gongdan_num,--工单数量
       0 as hetong_totalnum,                  --合同总数量
       0 as hetong_usernum,                   --合同客户数量
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
      from ex_ods_itr_workorder_work_order a
      where a.work_order_status <> 0
--         and a.gmt_create >= date_trunc('week', current_date - interval '12 week') -- 1 week,1 month,3 month
--         and a.gmt_create < date_trunc('week', current_date)
      group by a.product_id, a.product_type_name,
               date(a.gmt_create)) as a
         left join

     (select a.product_id                           as cpid,--产品ID,
             a.product_type_name                    as cptype,--产品类型,
             date(a.gmt_create)                     as event_date,--日期,
             array_to_string(ARRAY_AGG(a.id), ', ') AS gongdanid,-- 工单ID列表,
             'ITR工单客户数量'                      as yfkpi,--指标,
             count(distinct a.product_line_id)      as values--指标值
      from ex_ods_itr_workorder_work_order a
      where a.work_order_status <> 0
--         and a.gmt_create >= date_trunc('week', current_date - interval '12 week') -- 1 week,1 month,3 month
--         and a.gmt_create < date_trunc('week', current_date)
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
      from ex_ods_itr_workorder_work_order a
               inner join ex_ods_itr_workorder_flow_table b on a.id = b.biz_id
               inner join ex_ods_itr_workorder_flow_node c on b.id = c.flow_id
      where c.node_name = 'L3处理'
        and a.work_order_status <> 0
        and a.problem_big_type = 1560878489576247297
--         and a.gmt_create >= date_trunc('week', current_date - interval '12 week') -- 1 week,1 month,3 month
--         and a.gmt_create < date_trunc('week', current_date)
      group by a.product_id,
               a.product_type_name,
               date(a.gmt_create)) as c on a.event_date = c.日期 and a.cpid = c.cpid and a.cptype = c.产品类型

         left join
     (select a.product_id          as cpid,--产品ID,
             a.product_type_name   as 产品类型,
             date(a.gmt_create)    as 日期,
             array_to_string(ARRAY_AGG(case when c.node_name = 'L3处理' then a.id else null end),
                             ', ') AS gongdanid,-- 工单ID列表,
             'L3升单个数'        as 指标,
              count (distinct case when c.node_name = 'L3处理' then a.order_code else null end)             as 指标值
      from ex_ods_itr_workorder_work_order a
               inner join ex_ods_itr_workorder_flow_table b on a.id = b.biz_id
               inner join ex_ods_itr_workorder_flow_node c on b.id = c.flow_id
      where a.work_order_status <> 0
--         and a.gmt_create >= date_trunc('week', current_date - interval '12 week') -- 1 week,1 month,3 month
--         and a.gmt_create < date_trunc('week', current_date)
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
      from ex_ods_itr_workorder_work_order a
      where a.work_order_status <> 0
--         and gmt_create >= date_trunc('week', current_date - interval '12 week') -- 1 week,1 month,3 month
--         and gmt_create < date_trunc('week', current_date)
      group by a.product_id,
               date(a.gmt_create),
               a.product_type_name) as e on a.event_date = e.日期 and a.cpid = e.cpid and a.cptype = e.产品类型
group by a.event_date,
         a.cpid,
         a.cptype,
         a.itr_gongdan_num,
         b.values,--ITR工单客户数量
         c.指标值, --L3软件问题的工单数量
         d.指标值,--L3升单闭环率
         e.指标值,--工单数量
         a.gongdanid, --工单id列表
         b.gongdanid,
         c.gongdanid,
         d.gongdanid,
         e.gongdanid;

--2itr 研发/质量基础data 新增遗留分布
delete
from  ads_gp_pg_t_itr_newold_fenbu_df;
insert into ads_gp_pg_t_itr_newold_fenbu_df
select a.product_id                                                            as cpid,-- 工单ID,-- 增加产品ID，兼容后续
       a.product_type_name                                                     as cptype, --产品类型,
       Date(a.gmt_create)                                                      as event_date,--日期,
       a.product_version                                                       as banben_name,--版本名称,
       count(a.product_line_id)                                                as fankui_pro_qty,--反馈项目总数,
       array_to_string(ARRAY_AGG(case when a.product_line_id is null then null else a.id end),
                       ', ')                                                   AS pro_id,--反馈项目总数ID列表, --增加工单ID，用于产品下钻
       count(case when a.work_order_status not in (0, 5) then 1 else null end) as cp_wjj_qty,--产品内部未解决数,
       array_to_string(ARRAY_AGG(case when a.work_order_status not in (0, 5) then a.id else null end),
                       ', ')                                                   AS wjj_id,-- 产品内部未解决数工单ID列表, --增加工单ID，用于产品下钻
       count(distinct case
                          when a.work_order_status not in (0, 5) then a.product_line_id
                          else null end)                                       as yiliu_wjj_qty,--遗留未解决项目数,
       array_to_string(ARRAY_AGG(case
                                     when a.work_order_status not in (0, 5)
                                         then (case when a.product_line_id is null then null else a.id end)
                                     else null end),
                       ', ')                                                   as yiliu_id,-- 遗留未解决项目ID列表,
       ROUND((count(case when a.work_order_status not in (0, 5) then 1 else null end) /
              count(distinct a.id) :: NUMERIC), 2)                             as bihuan_lv,--闭环率,
       array_to_string(ARRAY_AGG(case when a.work_order_status not in (0, 5) then a.id else null end),
                       ', ')                                                   AS bhlv_id--闭环率工单ID列表 --增加工单ID，用于产品下钻
from ex_ods_itr_workorder_work_order a
where a.work_order_status <> 0
group by a.product_id, -- 增加产品ID，兼容后续
         a.product_type_name,
         a.product_version,
         Date(a.gmt_create);


--3itr 研发/质量基础data 用户问题分布
delete
from ads_gp_pg_t_itr_lastuser_fenbu_df;
insert into ads_gp_pg_t_itr_lastuser_fenbu_df
    (select a.product_id                           as cpid,--产品ID,
            a.product_type_name                    as cptype,--产品类型,
            date(a.gmt_create)                     as event_date,--日期,
            a.product_version                      as banben_name,--版本名称,
            a.ultimate_customer                    as last_user,--最终客户,
            array_to_string(ARRAY_AGG(a.id), ', ') as gongdanid,--测试工单ID列表,
            count(distinct a.ultimate_customer)    as gongdan_qty--工单数量
     from ex_ods_itr_workorder_work_order a
     where a.work_order_status <> 0
--    and a.ultimate_customer
     group by a.product_id,
              a.ultimate_customer,
              a.product_type_name,
              date(a.gmt_create),
              a.product_version);

--4itr 研发/质量基础data 问题类型分布
delete
from ads_gp_pg_t_itr_qtype_fenbu_df;
insert into ads_gp_pg_t_itr_qtype_fenbu_df
    (select a.product_id                           as cpid,--产品ID,
            a.product_type_name                    as cptype,--产品类型,
            date(a.gmt_create)                     as event_date,--日期,
            a.product_version                      as banben_name,--版本名称,
            b.show_name                            as qa_type,--问题类型,
            array_to_string(ARRAY_AGG(a.id), ', ') as gongdanid,--测试工单ID列表,
            count(b.show_name)                     as cp_qty--产品数量
     from ex_ods_itr_workorder_work_order a
              inner join ex_ods_itr_workorder_dictionary b on a.problem_small_type = b.id
     where a.work_order_status <> 0
--        and a.gmt_create >= date_trunc('week', current_date - interval '12 week') -- 1 week,1 month,3 month
--        and a.gmt_create < date_trunc('week', current_date)
     group by a.product_id,
              a.product_type_name,
              b.show_name,
              date(a.gmt_create),
              a.product_version);

--5itr 研发/质量基础data 工单等级分布
delete
from ads_gp_pg_t_itr_gongdan_fenbu_df;
insert into ads_gp_pg_t_itr_gongdan_fenbu_df
    (select a.product_id                           as cpid,--产品ID,
            a.product_type_name                    as cptype,--产品类型,
            date(a.gmt_create)                     as event_date,--日期,
            a.product_version                      as banben_inf,--版本信息,
            b.show_name                            as gongdan_level,--工单等级,
            array_to_string(ARRAY_AGG(a.id), ', ') as gongdanid,--测试工单ID列表,
            count(b.show_name)                     as gongdan_qty--工单数量
     from ex_ods_itr_workorder_work_order a
              inner join ex_ods_itr_workorder_dictionary b on a.order_level = b.id
     where a.work_order_status <> 0 --    and
           --    a.gmt_create >= '2022-12-02 00:00:00'
           --    and a.gmt_create <= '2022-12-08 23:00:59'
     group by a.product_id,
              a.product_type_name,
              b.show_name,
              date(a.gmt_create),
              a.product_version);


--6itr 研发/质量基础data 产品问题趋势
delete
from ads_gp_pg_t_itr_cp_qushi_df;
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
     from ex_ods_itr_workorder_work_order a
     where a.work_order_status <> 0
--        and a.gmt_create >= date_trunc('week', current_date - interval '12 week') -- 1 week,1 month,3 month
--        and a.gmt_create <  date_trunc('week', current_date)
     group by a.product_id,
              date(a.gmt_create),
              a.product_type_name);

--7itr 客户行业分布
delete
from ads_gp_pg_t_itr_user_hangye_df;
insert into ads_gp_pg_t_itr_user_hangye_df
    (with formson_table as (select "ID", formmain_id
                            from ex_ods_oa_abv5_formson_1939
                            union all
                            select "ID", formmain_id
                            from ex_ods_oa_abv5_formson_18956
                            union all
                            select "ID", formmain_id
                            from ex_ods_oa_abv5_formson_19389)
     select a.product_id                           as cpid,--产品ID,
            a.product_type_name                    as cptype,--产品类型,
            a.product_version                      as banben_name,--版本名称
            date(a.gmt_create)                     as event_date,--日期,
            --a.product_line_id                 as 合同号一需要匹配行业,
            c.field0380                            as first_cate,--第一行业,
            c.field0381                            as second_cate,--第二行业,
            array_to_string(ARRAY_AGG(a.id), ', ') AS gongdanid,--合同分布工单ID列表,
            count(distinct a.product_line_id)      as gongdan_qty--工单数量
     from ex_ods_itr_workorder_work_order a
              inner join formson_table b on product_line_id = cast(b."ID" as varchar(64))
              inner join ex_ods_oa_abv5_formmain_1119 c on b.formmain_id = c."ID"
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
delete
from ads_gp_pg_t_itr_gongdan_detail_df;
insert into  ads_gp_pg_t_itr_gongdan_detail_df
select distinct a.id as id ,--工单ID
a.order_code as id_code,--工单编号,,
a.order_title as gongdan_name,--工单标题,
a.product_id as cpid,--产品ID,
a.product_type_name as cptype,--产品类型,,
a.product_version as cpbanben,--产品版本,
b.show_name as qa_big,--问题大类
c.show_name as aq_small,--问题小类,
d.show_name as gongdan_level,--工单等级,
Date(a.gmt_create) as event_date,--时间,
f.hetong_id as contract_code,--合同编号,
f.kehu_name as last_user_name,--最终客户名称,
f.shangji_name as pro_name,--项目名称,
g.field0025 as cp_model,--产品型号,
g.field0026 as cp_module,--产品模块,
case
when a.work_order_status = 0 then '草稿'
when a.work_order_status = 1 then '未受理'
when a.work_order_status = 6 then '待派发'
when a.work_order_status = 2 then '受理中'
when a.work_order_status = 3 then '已处理'
when a.work_order_status = 5 then '已关闭'
else null end as gongdan_status --工单状态
from ex_ods_itr_workorder_work_order a
FULL OUTER JOIN ex_ods_itr_workorder_dictionary b on a.problem_big_type = b.id
FULL OUTER JOIN ex_ods_itr_workorder_dictionary c on a.problem_small_type = c.id
FULL OUTER JOIN ex_ods_itr_workorder_dictionary d on a.order_level = d.id
FULL OUTER JOIN (select "ID", formmain_id
from ex_ods_oa_abv5_formson_1939 a
union all
select "ID", formmain_id
from ex_ods_oa_abv5_formson_18956
union all
select "ID", formmain_id
from ex_ods_oa_abv5_formson_19389) e on a.product_line_id = cast(e."ID" as varchar(64))
FULL OUTER JOIN (select "ID", field0002 as hetong_id, field0004 as kehu_name, field0976 as shangji_name
from ex_ods_oa_abv5_formmain_1119
union all
select "ID", field0003 as hetong_id, field0005 as kehu_name, field0015 as shangji_name
from ex_ods_oa_abv5_formmain_18955) f on e.formmain_id = f."ID"
FULL OUTER JOIN ex_ods_oa_abv5_formson_1120 g on g.formmain_id = f."ID"
where a.work_order_status <> 0;


--9 itr挂起未响应订单
delete
from ads_gp_pg_t_itr_none_down_df;
insert into ads_gp_pg_t_itr_none_down_df
(
select Date(a.gmt_create)                                                                    as event_date,--日期,
       --写表的时候，这里要加两个字段,客户投诉数量,客户投诉详情
       a.product_id                                                                          as cpid,--产品ID,
       a.product_type_name                                                                      cptype, --as 产品类型,
       a.product_version                                                                        banben_inf,--as 版本信息,
       0                                                                                        none_num,--as 未响应工单量,
       ''                                                                                    as none_gongdanid,--未响应工单量ID列表, --增加工单ID，用于产品下钻
       count(case when b.hang_status = 0 then 1 else null end)                                  down_num,--as 挂起工单数,
       array_to_string(ARRAY_AGG(case when b.hang_status = 0 then a.id else null end), ', ') as down_gongdanid--挂起工单数ID列表 --增加工单ID，用于产品下钻
       --null                                                                                  as none_one,
       --null                                                                                  as none_two
from ex_ods_itr_workorder_work_order a
         inner join ex_ods_itr_workorder_hang_up b on a.id = b.order_id
where a.work_order_status <> 0
--    and a.gmt_create >= date_trunc('month', current_date - interval '3 month') -- 1 week,1 month,3 month
--    and a.gmt_create < date_trunc('month', current_date)
group by a.product_id,
         --增加产品ID，兼容后续
         a.product_version,
         a.product_type_name,
         Date(a.gmt_create)
union all
select Date(a.gmt_create)                     as 日期,
       --写表的时候，这里要加两个字段,客户投诉数量,客户投诉详情
       a.product_id                           as 产品ID,
       a.product_type_name                    as 产品类型,
       a.product_version                      as 版本信息,
       count(1)                               as 未响应工单量,
       array_to_string(ARRAY_AGG(a.id), ', ') as 未响应工单量ID列表,
       0                                      as 挂起工单数,
       ' '                                    as 挂起工单数ID列表
       --null                                   as none_one,
      -- null                                   as none_two
from ex_ods_itr_workorder_work_order a
         inner join ex_ods_itr_workorder_flow_table b on a.id = b.biz_id
         inner join ex_ods_itr_workorder_flow_node c on b.id = c.flow_id
    and b.current_node = c.id
where a.work_order_status <> 0
  and c.biz_status = 0
group by a.product_id,
         --增加产品ID，兼容后续
         a.product_version,
         a.product_type_name,
         Date(a.gmt_create));


--10itr 合同数据
delete
from ads_gp_pg_t_itr_hetong_qty_df;
insert into ads_gp_pg_t_itr_hetong_qty_df
select a.field0182                                   cpid,--     as 产品ID,
       count(distinct c.field0002)                   formal_hetong_num,--   as 正式合同产品数量,
       array_to_string(ARRAY_AGG(c.field0002), ', ') formal_hetong_id,--AS 正式合同产品数量列表,
       count(distinct c.field0004)                   formal_hetong_usernum,--as 正式合同客户数量,
       array_to_string(ARRAY_AGG(c.field0004), ', ') formal_hetong_user_id,--AS 正式合同客户数量列表
       null as                                       none_one,
       null as                                       none_two,
       null as                                       none_three
from ex_ods_oa_abv5_formmain_1294 a
         inner join ex_ods_oa_abv5_formson_1939 b on a.field0008 = b.field0174
         inner join ex_ods_oa_abv5_formmain_1119 c on b.formmain_id = c."ID"
group by a.field0182;



--12-2023 经营结果数据表
--delete  from ads_gp_all_t_cpx_cp_zhibiao_df where date(dateid)=date('2023-06-19');
--select * from ads_gp_all_t_cpx_cp_zhibiao_df where date(dateid)=date('2023-06-19');
insert into  ads_gp_all_t_cpx_cp_zhibiao_df
    (select usercode                                                                as dateid, --存储为日期分区
       cpxname,
       cpname,
       unnest(array ['合同产品金额','合同总数量','回款合同数量','订单成功率']) as yfkpi,--指标,
       unnest(array [合同产品金额,合同总数量,回款合同数量,订单成功率])         as values,--指标值
       cp_class
from (select (current_date - interval '1 days')           usercode,
             cpxname,
             cp_class,
             cpname,
             sum(hetong_gmv) / 1                       as 合同产品金额,
             sum(hetong_totalnum) / 1                  as 合同总数量,
             sum(hetong_huinum) / 1                    as 回款合同数量,
             sum(hetong_huinum) / sum(hetong_totalnum) as 订单成功率
      from ads_gp_oa_t_hangye_hetong_sales_df
      where date_part('year', event_date) = '2023'
      group by --usercode,
               cpxname,
               cp_class,
               cpname) as tt

union all

select usercode, cpx_name, cpname, 指标, 指标值, cp_class
from (select (current_date - interval '1 days') usercode,
             field0111      as                  cpname,
             field0065 cpx_name,
             null cp_class,
             '产品实际毛利' as                  指标,
             sum(field0046)                     指标值
      from ex_ods_oa_abv5_formmain_105321 as a
               left join ods_oa_finance_cp_map_relation as b on a.field0004 = b.item_code
      GROUP BY field0111,
               field0065) as t

union all
select usercode,
       t1.cpxname,
       t1.cpname,
       指标,
       指标值,
       cp_class
from (select (current_date - interval '1 days') usercode,
             cpxname,
             cp_class,
             cpname,
             '商机预计成交金额' as              指标,
             sum(yuji_gmv)      as              指标值
      from ads_gp_oa_t_hangye_shangji_sales_df
      where date_part('year', event_date) = '2023'
      group by --usercode,
               cpxname,
               cp_class,
               cpname) as t1

union all

select (current_date - interval '1 days') usercode,
       cpx_name                           cpxname,
       t1.cpname,
       '产品实际收入'  as                 指标,
       t1.产品实际收入 as                 指标值,
       cp_class
from (select field0111 as   cpname,
             field0065 cpx_name,
             null cp_class,
             sum(field0278) 产品实际收入
      from ex_ods_oa_abv5_formmain_105321 as a
               left join ods_oa_finance_cp_map_relation as b on a.field0004 = b.item_code
      GROUP BY field0111,
               field0065) as t1);
