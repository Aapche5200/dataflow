--硬件可扩展
select
c.name,
b.max_count
from
t_model a
LEFT JOIN t_rule_config b on a.id = b.model_id
LEFT JOIN t_module c on b.module_id = c.id
where
a.id = 1643163117319086081
and b.choice = 20
and c.type in (70,80)
order by b.rule



--性能规格
select
b.name,
b.value,
b.unit
from
t_model a
LEFT JOIN t_model_specification b on b.model_id = a.id
where
a.id = 1643163117319086081
and b.type = 1
order by b.sort