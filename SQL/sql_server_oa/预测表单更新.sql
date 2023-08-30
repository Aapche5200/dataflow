update formmain_129544
set field0018=gx.q2_done_lv,--q2完成率
    field0014=gx.q2_mubiao,--q2目标
    field0008=gx.q1_mubiao,--q1目标
    field0001=gx.confirm_date,--确认收入日期
    field0006=gx.current_income,--当前年累计确认收入
    field0007=gx.current_unincome,--当前年累计待确认收入
    field0005=gx.year_mubiao,--年目标
    field0035=gx.four_m_income,--4月实际收入
    field0016=gx.q2_income,--q2实际收入
    field0036=gx.five_m_income,--5月实际收入
    field0049=gx.seven_m_income,--7月实际收入
    field0050=gx.eight_m_income,--8月实际收入
    field0063=gx.ten_m_income,--10月实际收入
    field0064=gx.eleven_m_income,--11月实际收入
    field0015=gx.q2_predict_income,--q2预测收入
    field0021=gx.q3_predict_income,--q3预测收入
    field0027=gx.q4_predict_income,--q4预测收入
    field0022 = gx.q3_income, --Q3实际收入
    field0020=gx.q3_mubiao, --	Q3收入目标
    field0028= gx.q4_income,--Q4实际收入
    field0026=gx.q4_mubiao, --	Q4收入目标
    field0017=gx.q2_predict_done_lv, --	Q2预测完成率
    field0023=gx.q3_predict_done_lv, --	Q3预测完成率
    field0024=gx.q3_done_lv, --	Q3收入完成率
    field0029=gx.q4_predict_done_lv, --		Q4预测完成率
    field0030=gx.q4_done_lv, --	Q4收入完成率
    field0089=gx.six_m_income, --	六月实际收入
    field0090=gx.nine_m_income, --	九月实际收入
    field0091=gx.twelve_m_income --	十二月实际收入
from (select event_date    as                    confirm_date,
             t1.area,
             t1.area_unit,
             t1.date_y,
             round(t1.current_income / 10000, 2) current_income,
             round(t4.unincome / 10000, 2)       current_unincome,
             round(t1.q1_income / 10000, 2)      q1_income,
             round(t2.q1_mubiao / 10000, 2)      q1_mubiao,
             round(q1_income / t2.q1_mubiao, 4)  q1_done_lv,
             round(t2.year_mubiao / 10000, 2)    year_mubiao,
             round((t2.year_mubiao * 0.32 - t1.q1_income) / 10000,
                   2)      as                    q2_mubiao,
             round(t1.q2_income / 10000, 2)      q2_income,
             round(t1.q2_income / (t2.year_mubiao * 0.32 - t1.q1_income),
                   4)                            q2_done_lv,
             round(t1.q3_income / 10000, 2)      q3_income,
             iif(q3_income = 0, 0,
                 round((t2.year_mubiao * 0.6 - q1_income - q2_income) / 10000,
                       2)) as                    q3_mubiao,
             round(q3_income / (t2.year_mubiao * 0.6 - q1_income - q2_income),
                   4)      as                    q3_done_lv,
             round(t1.q4_income / 10000, 2)      q4_income,
             iif(q4_income = 0, 0,
                 round((t2.year_mubiao - q1_income - q2_income - q3_income) / 10000,
                       2)) as                    q4_mubiao,
             round(q4_income / (t2.year_mubiao - q1_income - q2_income - q3_income),
                   4)      as                    q4_done_lv,
             round(t1.four_m_income / 10000, 2)  four_m_income,
             round(five_m_income / 10000, 2)     five_m_income,
             round(six_m_income / 10000, 2)      six_m_income,
             round(seven_m_income / 10000, 2)    seven_m_income,
             round(eight_m_income / 10000, 2)    eight_m_income,
             round(nine_m_income / 10000, 2)     nine_m_income,
             round(ten_m_income / 10000, 2)      ten_m_income,
             round(eleven_m_income / 10000, 2)   eleven_m_income,
             round(twelve_m_income / 10000, 2)   twelve_m_income,
             t3.q2_predict_income,
             round((t3.q2_predict_income * 10000) /
                   ((t2.year_mubiao * 0.32 - t1.q1_income)),
                   4)                            q2_predict_done_lv,
             t3.q3_predict_income,
             round((t3.q3_predict_income * 10000) /
                   (t2.year_mubiao * 0.6 - q1_income - q2_income),
                   4)                            q3_predict_done_lv,
             t3.q4_predict_income,
             round((t3.q4_predict_income * 10000) /
                   (t2.year_mubiao - q1_income - q2_income - q3_income),
                   4)                            q4_predict_done_lv
      from (--增量计算
               select cast(area as varchar(800))      as area,
                      cast(area_unit as varchar(800)) as area_unit,
                      cast(date_y as varchar(800))    as date_y,
                      max(confirm_date)               as confirm_date,
                      sum(coalesce(income_diff, 0))   as current_income,
                      sum(coalesce(unincome_diff, 0)) as current_unincome,
                      sum(case
                              when cast(date_q as varchar(800)) = 'Q1' then income_diff
                              else 0 end)             as q1_income,
                      sum(case
                              when cast(date_q as varchar(800)) = 'Q2' then income_diff
                              else 0 end)             as q2_income,
                      sum(case
                              when cast(date_q as varchar(800)) = 'Q3' then income_diff
                              else 0 end)             as q3_income,
                      sum(case
                              when cast(date_q as varchar(800)) = 'Q4' then income_diff
                              else 0 end)             as q4_income,
                      sum(case
                              when cast(date_m as varchar(800)) = '4月' then income_diff
                              else 0 end)             as four_m_income,
                      sum(case
                              when cast(date_m as varchar(800)) = '5月' then income_diff
                              else 0 end)             as five_m_income,
                      sum(case
                              when cast(date_m as varchar(800)) = '6月' then income_diff
                              else 0 end)             as six_m_income,
                      sum(case
                              when cast(date_m as varchar(800)) = '7月' then income_diff
                              else 0 end)             as seven_m_income,
                      sum(case
                              when cast(date_m as varchar(800)) = '8月' then income_diff
                              else 0 end)             as eight_m_income,
                      sum(case
                              when cast(date_m as varchar(800)) = '9月' then income_diff
                              else 0 end)             as nine_m_income,
                      sum(case
                              when cast(date_m as varchar(800)) = '10月' then income_diff
                              else 0 end)             as ten_m_income,
                      sum(case
                              when cast(date_m as varchar(800)) = '11月' then income_diff
                              else 0 end)             as eleven_m_income,
                      sum(case
                              when cast(date_m as varchar(800)) = '12月' then income_diff
                              else 0 end)             as twelve_m_income
               from ads_gp_oa_t_area_unit_sales_dd
               group by cast(area as varchar(800))
                      , cast(area_unit as varchar(800))
                      , cast(date_y as varchar(800))) as t1
               left join( --目标
          select 大区名称 as                  大区,
                 "NAME"   as                  办事处,
                 全年收入目标 * 10000         year_mubiao,
                 全年收入目标 * 10000 * 0.128 q1_mubiao
          from (select 大区名称, "三级部门ID", sum(field0044) 全年收入目标
                from formmain_5137 as t1
                         left join saleDepartLevel as t2 on t1.field0002 = t2."销售部门ID"
                where 大区名称 in ('北区管理部', '南区管理部')
                  and t1.field0004 = 2023
                  and t1.field0007 = '8559535201370522547'
                  and t1.field0044 is not null
                group by 大区名称, "三级部门ID") as t1
                   join org_unit as t2 on t1."三级部门ID" = t2."ID"

          union all

          SELECT 大区名称,
                 case when "NAME" = '合肥办' then '安徽办' else "NAME" END name,
                 全年收入目标 * 10000                                      year_mubiao,
                 全年收入目标 * 10000 * 0.128                              q1_mubiao
          FROM (select field0002, "NAME", sum(field0044) 全年收入目标
                from formmain_5137 as t1
                         join org_unit as t2 on t1.field0002 = t2."ID"
                WHERE field0004 = 2023
                  and field0007 = '8559535201370522547'
                  and field0044 is not null
                group by field0002, "NAME") AS t1
                   join saleDepartLevel as t2 on t1.field0002 = t2."销售部门ID"
          where 大区名称 in ('北分管理部', '长沙办', '上海大区', '战略合作部')) as t2
                        on t1.area = t2.大区 and t1.area_unit = t2.办事处
               left join ( --预测
          select field0002,
                 field0003,
                 field0004,
                 sum(coalesce(field0042, 0)) +
                 sum(coalesce(field0043, 0)) +
                 sum(coalesce(field0044, 0)) +
                 sum(coalesce(field0045, 0)) +
                 sum(coalesce(field0046, 0)) +
                 sum(coalesce(field0047, 0)) +
                 sum(coalesce(field0048, 0)) as q2_predict_income,
                 sum(coalesce(field0056, 0)) +
                 sum(coalesce(field0057, 0)) +
                 sum(coalesce(field0058, 0)) +
                 sum(coalesce(field0059, 0)) +
                 sum(coalesce(field0060, 0)) +
                 sum(coalesce(field0061, 0)) +
                 sum(coalesce(field0062, 0)) as q3_predict_income,
                 sum(coalesce(field0070, 0)) +
                 sum(coalesce(field0071, 0)) +
                 sum(coalesce(field0072, 0)) +
                 sum(coalesce(field0073, 0)) +
                 sum(coalesce(field0074, 0)) +
                 sum(coalesce(field0075, 0)) +
                 sum(coalesce(field0076, 0)) as q4_predict_income
          from formmain_129544
          group by field0002, field0003, field0004) as t3
                         on t1.area = t3.field0002 and t1.area_unit = t3.field0003 and
                            t1.date_y = t3.field0004
               left join (select case
                                     when field0011 = '全国运营商行业部' then '其它（营销中心）'
                                     when field0011 = '北京渠道事业部' then '北分管理部'
                                     when field0011 in ('北京网安', '金华总经办', '数瀚项目实施部', '综合业务部')
                                         then '其它（非营销中心）'
                                     when field0001 is null then '其他'
                                     else field0001 end         area,


                                 case
                                     when field0011 = '北京渠道事业部' then '渠道事业部(北京分公司)'
                                     when field0011 = '军民融合事业部二部' then '军民融合事业二部'
                                     when field0011 = '上海浦东新监管事业部'
                                         then '上海新监管浦东事业部'
                                     else field0011 end
                                                                area_unit,
                                 sum(coalesce(field0045, 0)) +
                                 sum(coalesce(field0043, 0)) +
                                 sum(coalesce(field0044, 0)) +
                                 sum(coalesce(field0046, 0)) as unincome
                          from formmain_128838 as t1
                          where field0104 is not null
                            and (coalesce(field0005, '其他') <> '已注销'
                              and coalesce(field0017, '其他') <> '内部结算')
                          group by case
                                       when field0011 = '全国运营商行业部' then '其它（营销中心）'
                                       when field0011 = '北京渠道事业部' then '北分管理部'
                                       when field0011 in ('北京网安', '金华总经办', '数瀚项目实施部', '综合业务部')
                                           then '其它（非营销中心）'
                                       when field0001 is null then '其他'
                                       else field0001 end,
                                   case
                                       when field0011 = '北京渠道事业部'
                                           then '渠道事业部(北京分公司)'
                                       when field0011 = '军民融合事业部二部'
                                           then '军民融合事业二部'
                                       when field0011 = '上海浦东新监管事业部'
                                           then '上海新监管浦东事业部'
                                       else field0011 end) as t4
                         on t1.area = t4.area and t1.area_unit = t4.area_unit
               left join (select max(convert(varchar(100), field0297, 23)) as event_date
                          from formmain_105321) as t5
                         on 1 = 1) as gx
where field0002 = gx.area
  and field0003 = gx.area_unit;

