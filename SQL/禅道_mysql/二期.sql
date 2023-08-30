select id,COMMENT,
  SUBSTRING_INDEX(SUBSTRING_INDEX(COMMENT, '||', 1), '||', -1) AS value1,
  SUBSTRING_INDEX(SUBSTRING_INDEX(COMMENT, '||', 2), '||', -1) AS value2,
  SUBSTRING_INDEX(SUBSTRING_INDEX(COMMENT, '||', 3), '||', -1) AS value3
    from(
SELECT b.id id,
       GROUP_CONCAT(a.comment,'||') AS COMMENT
from zt_bug b
INNER JOIN zt_action a on a.objectID = b.id
where b.deleted = '0'
and a.comment <>''
group by b.id
) as t


select a.account,
       a.realname,
       dept,
       b.name,
       c.name,
       d.name,
       e.name,
       f.name,
       count(a.account)
from zt_user as a
         left join zt_dept as b on a.dept = b.id
         left join zt_dept as c on b.parent = c.id
         left join zt_dept as d on c.parent = d.id
         left join zt_dept as e on d.parent = e.id
         left join zt_dept as f on e.parent = f.id
         left join zt_attend as t on a.account = t.account
group by a.account,
         a.realname,
         dept,
         b.name,
         c.name,
         d.name,
         e.name,
         f.name
select * from zt_dept