select case
           when yuji_gmv < 50 then '<50'
           when yuji_gmv > 50 and yuji_gmv <= 100 then '50-100'
           when yuji_gmv > 100 and yuji_gmv <= 200 then '100-200'
           when yuji_gmv > 200 and yuji_gmv <=500 then '200-500'
           when yuji_gmv > 500 and yuji_gmv <= 1000 then '500-1000'
           else null
           end tag,
       sum(yuji_gmv)
from ads_gp_oa_t_hangye_shangji_sales_df
where date_part('year', event_date) = '2023'
  and cpname in (select distinct cpname from ads_gp_rm_t_hr_cp_inf_df where cpname != '')
  and cpxname in (select distinct cpxname from ads_gp_rm_t_hr_cp_inf_df where cpname != '')
  --and cpname='明御WEB应用防火墙'
group by case
           when yuji_gmv < 50 then '<50'
           when yuji_gmv > 50 and yuji_gmv <= 100 then '50-100'
           when yuji_gmv > 100 and yuji_gmv <= 200 then '100-200'
           when yuji_gmv > 200 and yuji_gmv <=500 then '200-500'
           when yuji_gmv > 500 and yuji_gmv <= 1000 then '500-1000'
           else null
           end



select
      sum(yuji_gmv)
from ads_gp_oa_t_hangye_shangji_sales_df
where date(event_date) >= date('2023-01-01') and date(event_date) <= date('2023-12-31') and
  cpname in (select distinct cpname from ads_gp_rm_t_hr_cp_inf_df where cpname != '')
  and cpxname in (select distinct cpxname from ads_gp_rm_t_hr_cp_inf_df where cpname != '') and  cpname='明御云WEB应用防火墙'

select
      sum(yuji_gmv)
from ads_gp_oa_t_hangye_shangji_sales_df
where date(event_date) >= date('2023-05-14') and date(event_date) <= date('2023-12-31') and
  cpname in (select distinct cpname from ads_gp_rm_t_hr_cp_inf_df where cpname != '')
  and cpxname in (select distinct cpxname from ads_gp_rm_t_hr_cp_inf_df where cpname != '') and  cpname='明御云WEB应用防火墙'


select
     pro_status,sum(yuji_gmv)
from ads_gp_oa_t_hangye_shangji_sales_df
where  date_part('year', event_date) = '2023' and
  cpname in (select distinct cpname from ads_gp_rm_t_hr_cp_inf_df where cpname != '')
  and cpxname in (select distinct cpxname from ads_gp_rm_t_hr_cp_inf_df where cpname != '') --and cpname='明御WEB应用防火墙'
group by pro_status



select EXTRACT('QUARTER' FROM event_date) as 季度,
     pro_status,sum(yuji_gmv)
from ads_gp_oa_t_hangye_shangji_sales_df
where  date_part('year', event_date) = '2023' and
  cpname in (select distinct cpname from ads_gp_rm_t_hr_cp_inf_df where cpname != '')
  and cpxname in (select distinct cpxname from ads_gp_rm_t_hr_cp_inf_df where cpname != '') and cpname='明御WEB应用防火墙'
group by EXTRACT('QUARTER' FROM event_date) ,pro_status




select cpxname,yfkpi,sum(values) from ads_gp_all_t_cpx_cp_zhibiao_df
where date(dateid)=date('2023-04-12') --and yfkpi='产品实际收入'
  and cpxname in ('物联网+产品线', '基础安全产品线', 'AiLPHA大数据智能安全产品线', '云产品线')
 -- and cpname='明御WEB应用防火墙'
 group by cpxname ,yfkpi



select substr(to_char(event_date,'YYYY-MM-DD'),1,7),
     cpxname --,cpname
     , sum(hetong_gmv)from
ads_gp_oa_t_hangye_hetong_sales_df
where date(event_date)>=date('2022-01-01') and date(event_date)<=date('2023-04-27')
and cpxname in ('智能检测与终端产品线', '基础安全产品线', 'AiLPHA大数据智能安全产品线', '云产品线') --and cpname='政企态势'
group by substr(to_char(event_date,'YYYY-MM-DD'),1,7),
    cpxname --,cpname




select --substr(to_char(event_date, 'YYYY-MM-DD'), 1, 7),
       banshichu_name,
       sum(hetong_cp_gmv)
from ads_gp_oa_t_banshichu_hetong_sales_df
where substr(to_char(event_date,'YYYY-MM-DD'),1,4) = '2022'  and cpxname in ('智能检测与终端产品线', '基础安全产品线', 'AiLPHA大数据智能安全产品线', '云产品线')
and cpname='安恒网络风险扫描与管理系统'
group by --substr(to_char(event_date, 'YYYY-MM-DD'), 1, 7),
banshichu_name

select cpxname,cpname, yfkpi, sum(values)
from ads_gp_all_t_cpx_cp_zhibiao_df
where date(dateid) = date('2023-04-12')
  and cpname in (select distinct cpname from ads_gp_rm_t_hr_cp_inf_df where cpname!='' and cpxid in ('02'))
and cpxname in (select distinct cpxname from ads_gp_rm_t_hr_cp_inf_df where cpname!='' and cpxid in ('02'))
group by cpxname, cpname,yfkpi


select pro_tag,sum(yuji_gmv)
  from  ads_oa_t_shangji_status_sales_df
where date(event_date) >= date('2023-01-01') and date(event_date) <= date('2023-12-31')
     and cpname in (select distinct cpname from ads_gp_rm_t_hr_cp_inf_df where cpname!='')
and cpid in (select distinct cpxname from ads_gp_rm_t_hr_cp_inf_df where cpname!='')
and cpid in ('物联网+产品线', '基础安全产品线', 'AiLPHA大数据智能安全产品线', '云产品线') and cpname='明御WEB应用防火墙'
group by pro_tag


select sum(case  pro_tag when '商机' then yuji_gmv else 0 end),
       sum(case  pro_tag when '信息' then yuji_gmv else 0 end),
       sum(case  pro_tag when '争取' then yuji_gmv else 0 end),
       sum(case  pro_tag when '承诺' then yuji_gmv else 0 end),
       sum(case  pro_tag when '中标' then yuji_gmv else 0 end)
       --sum(case  pro_tag when '商机' then yuji_gmv else 0 end),
  from  ads_oa_t_shangji_status_sales_df
where date(event_date) >= date('2023-01-01') and date(event_date) <= date('2023-12-31')
     and cpname in (select distinct cpname from ads_gp_rm_t_hr_cp_inf_df where cpname!='')
and cpid in (select distinct cpxname from ads_gp_rm_t_hr_cp_inf_df where cpname!='')
and cpid in ('物联网+产品线', '基础安全产品线', 'AiLPHA大数据智能安全产品线', '云产品线')and cpname='明鉴Web应用弱点扫描器'



select * from ads_oa_t_shangji_status_sales_df where cpid in ('物联网+产品线', '基础安全产品线', 'AiLPHA大数据智能安全产品线', '云产品线') and
date(event_date) >= date('2023-01-01') and date(event_date) <= date('2023-12-31')


select
    --substr(to_char(event_date,'YYYY-MM-DD'),1,7)
    usercode
    ,sum(yuji_gmv)
    from
ads_gp_oa_t_banshichu_shangji_sales_df
where date(event_date) >= date('2023-01-01') and date(event_date) <= date('2023-12-31')
   and cpname in (select distinct cpname from ads_gp_rm_t_hr_cp_inf_df where cpname!='')
and cpxname in (select distinct cpxname from ads_gp_rm_t_hr_cp_inf_df where cpxname!='')
--and cpxname =''
 -- and cpname='明御WEB应用防火墙'
group by --substr(to_char(event_date,'YYYY-MM-DD'),1,7),
         usercode


select
    substr(to_char(event_date,'YYYY-MM-DD'),1,7)
    --usercode
--     sum(case when usercode=cast(0 as text) then '0' else null end),
--      sum(case when usercode=cast(25 as text) then '25' else null end),
--      sum(case when usercode=cast(50 as text) then '50' else null end),
--      sum(case when usercode=cast(75 as text) then '75' else null end),
--      sum(case when usercode=cast(100 as text) then '100' else null end)
    ,sum(yuji_gmv)
    from
ads_gp_oa_t_banshichu_shangji_sales_df
where date(event_date) >= date('2023-01-01') and date(event_date) <= date('2023-12-31')
   and cpname in (select distinct cpname from ads_gp_rm_t_hr_cp_inf_df where cpname!='')
and cpxname in (select distinct cpxname from ads_gp_rm_t_hr_cp_inf_df where cpxname!='')
--and cpxname =''
  and cpname='明御WEB应用防火墙'
group by substr(to_char(event_date,'YYYY-MM-DD'),1,7)
--,usercode


select em_name,
       sum(hetong_gmv)
from ads_oa_t_sales_em_sales_df
where date(event_date) >= date('2023-01-01')
  and cpx_name in ('物联网+产品线', '基础安全产品线', 'AiLPHA大数据智能安全产品线', '云产品线') and cpname='安恒云-在线SaaS'
group by em_name





select * from ads_gp_oa_t_hetong_shangji_qty_df
where substr(event_date,1,4)='2022' -- and cpid in ('物联网+产品线', '基础安全产品线', 'AiLPHA大数据智能安全产品线', '云产品线')
  and win_lv<>'非赢单率'
  and cpname='total'
  and cpid='total'




select * from ads_gp_oa_t_hetong_shangji_qty_df
where substr(event_date,1,4)='2022' --and cpid in ('物联网+产品线', '基础安全产品线', 'AiLPHA大数据智能安全产品线', '云产品线')
  and win_lv='非赢单率'
 and cpname='total'
and cpid='total'




select cpxname,cpname,sum(hetong_gmv),sum(cp_maoli) from ads_gp_oa_t_hangye_hetong_sales_df
where substr(to_char(event_date,'YYYY-MM-DD'),1,4)='2023' and  cpxname in ('物联网+产品线', '基础安全产品线', 'AiLPHA大数据智能安全产品线', '云产品线')
group by  cpxname,cpname



select substr(to_char(event_date, 'YYYY-MM-DD'), 1, 4)
    -- , cpname
     , sum(purchase_qty)
from ads_gp_oa_t_hangye_hetong_sales_df
where date(event_date) >= date('2022-01-01')
  and date(event_date) <= date('2023-04-14')
  and cpxname in ('物联网+产品线', '基础安全产品线', 'AiLPHA大数据智能安全产品线', '云产品线')
  --and cpname = 'APT'
group by substr(to_char(event_date, 'YYYY-MM-DD'), 1, 4)--, cpname

select event_date,sum(purchase_qty)
from  ads_gp_oa_t_cp_hetong_qty_df --limit 10  ads_gp_oa_t_cp_hetong_qty_df
where cpxname in ('物联网+产品线', '基础安全产品线', 'AiLPHA大数据智能安全产品线', '云产品线')
 -- and cpname = 'APT'
  group by event_date



select substr(to_char(event_date,'YYYY-MM-DD'), 1, 7), sum(hetong_gmv)
from ads_gp_oa_t_hangye_hetong_sales_df
where cpname='政企态势'
group by substr(to_char(event_date,'YYYY-MM-DD'), 1, 7)


select * from
ads_gp_all_t_cpx_cp_zhibiao_df




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
      GROUP BY date(field0028),
               --field0010,
               field0091,
               field0092,
               field0275, b.cp_name, b.cpx_name,b.cp_class) as t1;