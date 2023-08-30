ALTER TABLE shushantest
ADD PARTITION BY (date_id string);

ALTER TABLE shushantest
    SET TABLESPACE partition_table_space, -- 可选项，指定表空间
    ADD PRIMARY KEY (id),
    ADD CONSTRAINT partition_key_check CHECK (partition_key >= 0),
    ALTER COLUMN partition_key SET NOT NULL,
    PARTITION BY RANGE (date_id);

--例如：ads_gp_oa_t_hangye_hetong_sales_df
SELECT *
from ds_oa_formmain_10037
limit 10
--测试示例

delete from shushantest;
drop table gp_oa_hangye_hetong_sales_df
create table shushantest as
(select '日期' as test)

insert into shushantest --(date_id='2023-01-01')
select '日期2' as test
select * from shushantest where date_id=date('2023-03-21')



--涉及到数量，如果做累加会导致重复计算问题
---员工基本信息--gp_oa_hr_em_inf_df
insert overwrite table ads_gp_oa_t_hr_eminf_inf_df
create table ads_gp_oa_t_hr_eminf_inf_df as
select count(distinct field0002) as 在职员工数
 from ds_oa_formmain_10037 as t1
         left join ds_oa_depart_level_all as t2 on t1.field0200 = cast(t2."部门ID" as text)
where field0180 = '-7808949530931608431'
  and t2.一级部门 in ('智能检测与终端产品线', '基础安全产品线', 'AiLPHA大数据智能安全产品线', '云产品线')

---行业 合同维度 gp_oa_hangye_hetong_sales_df
insert overwrite table ads_gp_oa_t_hangye_hetong_sales_df
create table ads_gp_oa_t_hangye_hetong_sales_df as
select sum(产品实际毛利),sum(合同产品金额),sum(合同客户数),sum(合同总数量) ,count(工号) from(
select t1.日期,
             t1.工号,
             --t2.产品线名称,
            -- t2.产品名称,
             t1.一级行业,
             t1.二级行业,
             t1.三级行业,
             t1.产品实际毛利,
             t1.合同产品金额,
             t1.合同客户数,
             t1.合同总数量,
             t1.回款合同数量
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
                     field0275) as t1) as t


---办事处 合同维度 gp_oa_banshichu_hetong_sales_df
insert overwrite table ads_gp_oa_t_banshichu_hetong_sales_df
create table ads_gp_oa_t_banshichu_hetong_sales_df as
select tt1.日期,
             tt1.工号,
             tt2.产品线名称,
             tt2.产品名称,
             tt1.办事处ID,
             tt1.办事处名称,
             tt1.产品实际毛利,
             tt1.合同产品金额,
             tt1.合同客户数,
             tt1.合同总数量,
             tt1.回款合同数量
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
                 (
                 select t1.cpx      需求表产品线ID,
                         t1.cpxcpbd  需求表产品ID,
                         t2.cpxmc as 产品线名称,
                         t3.name  as 产品名称
                  from ds_ecology_uf_IPDxqgl as t1
                           left join ds_ecology_uf_productline as t2 on t1.cpx = t2.cpxid
                           left join ds_ecology_uf_product as t3 on t1.cpxcpbd = t3.cpid
                  group by t1.cpx, t1.cpxcpbd, t2.cpxmc, t3.name
                  ) as t2 on t1.id = t2.需求表产品ID) as tt2
           on tt1.工号 = tt2.员工工号



---行业 商机
insert overwrite table ads_gp_oa_t_hangye_shangji_sales_df
create table ads_gp_oa_t_hangye_shangji_sales_df as
select tt1.日期,
       tt1.工号,
       tt2.产品线名称,
       tt2.产品名称,
       tt1.一级行业名称,
       tt1.二级行业名称,
       tt1.三级行业名称,
       tt1.状态,
       tt1.预计成交金额
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
     on tt1.工号 = tt2.员工工号


---办事处 商机
insert overwrite table from ads_gp_oa_t_banshichu_shangji_sales_df
create table ads_gp_oa_t_banshichu_shangji_sales_df as
select tt1.日期,
       tt1.工号,
       tt2.产品线名称,
       tt2.产品名称,
       tt1.办事处ID,
       tt1.办事处名称,
       tt1.状态,
       tt1.预计成交金额
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
     on tt1.工号 = tt2.员工工号

---测试合同
insert overwrite table ads_gp_oa_t_ceshi_hetong_qty_df
create table ads_gp_oa_t_ceshi_hetong_qty_df as
select tt1.日期,
       tt1.工号,
       tt2.产品线名称,
       tt2.产品名称,
       tt1.测试合同数量
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
                  where date(t1.start_date) >= date('2022-01-01') and date(t1.start_date) < date(current_date)) AS tt1
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
     on tt1.工号 = tt2.员工工号

---RM评审数据
insert overwrite table ads_gp_rm_t_pingsheng_hetong_qty_df
create table ads_gp_rm_t_pingsheng_hetong_qty_df as
select 日期,
       员工工号,
       产品线名称,
       产品名称,
       COUNT(distinct id)                                         AS 总数量,
       count(distinct (CASE WHEN xqzt = 0 THEN id ELSE null END)) AS 未处理需求数量
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
         产品名称



--员工产品线，产品表
create table ads_gp_rm_t_hr_cp_inf_df as
insert overwrite table ads_gp_rm_t_hr_cp_inf_df
select 员工工号,
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
      group by t1.cpx, t1.cpxcpbd, t2.cpxmc, t3.name) as t2 on t1.id = t2.需求表产品ID






---2023 经营结果数据表
--KPI&经营---商机达成率--产品实际收入
create table ads_gp_all_t_cpx_cp_zhibiao_df as
(
select 工号,
       产品线名称,
       产品名称,
       unnest(array ['产品实际毛利','合同产品金额','合同总数量','回款合同数量','订单成功率']) as 指标,
       unnest(array [产品实际毛利,合同产品金额,合同总数量,回款合同数量,订单成功率])           as 指标值
from (select 工号,
             产品线名称,
             产品名称,
             sum(产品实际毛利) / 1               as 产品实际毛利,
             sum(合同产品金额) / 1               as 合同产品金额,
             sum(合同总数量) / 1                 as 合同总数量,
             sum(回款合同数量) / 1               as 回款合同数量,
             sum(回款合同数量) / sum(合同总数量) as 订单成功率
from ads_gp_oa_t_hangye_hetong_sales_df
      where date_part('year', 日期) = '2023'
      group by 工号,
               产品线名称,
               产品名称) as tt
union all
select 工号,
       产品线名称,
       产品名称,
       '商机预计成交金额' as 指标,
       sum(预计成交金额)  as 指标值
from ads_gp_oa_t_hangye_shangji_sales_df
where date_part('year', 日期) = '2023'
group by 工号,
         产品线名称,
         产品名称
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
     on t1.工号 = t2.员工工号
)) as tt

--预算消耗率
--见第一部分OA表

--RM闭环率
--见大宽表部分

--
-- ads_gp_all_t_cpx_cp_zhibiao_df
-- (以下表跑完，此表才会更新)
--
-- ads_gp_oa_t_banshichu_hetong_sales_df
-- ads_gp_oa_t_banshichu_shangji_sales_df
-- ads_gp_oa_t_ceshi_hetong_qty_df
-- ads_gp_oa_t_hangye_hetong_sales_df
-- ads_gp_oa_t_hangye_shangji_sales_df
-- ads_gp_oa_t_hr_eminf_inf_df
-- ads_gp_rm_t_hr_cp_inf_df
-- ads_gp_rm_t_pingsheng_hetong_qty_df