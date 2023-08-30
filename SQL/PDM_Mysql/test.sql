select distinct xhno, specs, name, no
from mpart
where specs IN ('DAS-AHCloud-S-OMA50-QD')


select distinct xhno, specs, name, no
from mpart
where no IN ('CP030528-00063')

select no,name,specs  from  fprodlist
group by no,name,specs ,xhno

select * from fproduct limit 10
select  from fprodlist limit 10

/*    fprodlist   型模表
    fproduct 型号产品表
    mpart 物料表*/

select distinct fl.no as lpbm,fl.name,fp.cpxh,fp.no as xhno  from  fprodlist as fl
join fproduct as fp on fl.fproductid=fp.id
where fl.Wkaid='1'





select distinct xhno, specs, name, no, properfield_7
from mpart
where properfield_7 like '许可%'


select * from mpart limit 10