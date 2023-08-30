select distinct fl.no as lpbm, fl.name, fp.cpxh, fp.no as xhno
from fprodlist as fl
         join fproduct as fp on fl.fproductid = fp.id
where fl.Wkaid = '1'

