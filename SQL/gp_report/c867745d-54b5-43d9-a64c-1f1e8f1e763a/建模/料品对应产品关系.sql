-- field0001	产品型号编码
-- 		field0003	产品模块料号
-- formmain_114136 t2

-- formmain_114093  t1
-- field0005	产品型号编码
-- 	field0003	产品ID
-- 	field0004	产品名称
-- field0001	产品线ID
-- field0002	产品线名称

-- 1294：a
--  field0008 as 料品编码,
--     field0001	产品名称
-- field0002	产品型号
-- field0003	产品模块

select a.field0008 as 料品编码,
       a.field0001 as 历史产品名称,
       a.field0002 as 历史产品型号,
       a.field0003 as 历史产品模块,
       b.field0005 as 当前产品型号编码,
       b.field0003 as 当前产品ID,
       b.field0004 as 当前产品名称,
       b.field0001 as 当前产品线ID,
       b.field0002 as 当前产品线名称,
       null        as 当前产品规格,
       null        as 当前产品模块,
       null        as 当前负责人所有在部门,
       null        as "当前负责人工号-可多填写-斜杠区分"
from (select field0008,
             field0001,
             field0002,
             field0003
  select count(distinct field0008)    from ex_ods_oa_abv5_formmain_1294
      group by field0008,
               field0001,
               field0002,
               field0003) as a
         left join (select t1.field0005,
                           t1.field0003,
                           t1.field0004,
                           t2.field0003 as lihao,
                           t1.field0001,
                           t1.field0002
                    from ex_ods_oa_abv5_formmain_114093 as t1
                              join ex_ods_oa_abv5_formmain_114136 as t2
                                  on cast(t1.field0005 as text) = cast(t2.field0001 as text)
                    group by t1.field0005, t1.field0003, t1.field0004, t2.field0003, t1.field0001,
                             t1.field0002) as b on a.field0008 = b.lihao


select distinct t1.field0005,
                t1.field0003,
                t1.field0004,
                t1.field0001,
                t1.field0002
from ex_ods_oa_abv5_formmain_114093 as t1    --产品型号编码


select distinct t2.field0001,t2.field0003 from ex_ods_oa_abv5_formmain_114136 as t2  --型号编码