select * from ads_gp_oa_t_area_unit_sales_dd
create table ads_gp_oa_t_area_unit_sales_dd
(
date_y text,--年份,
date_q text,--季度,
date_m text,--月度,
confirm_date date ,--确认日期,
area text,--大区,
area_unit text,--考核单位,
income_diff numeric,-- 年度累计确认收入差值,
unincome_diff numeric --当前待确认收入差值

);
go
exec sp_addextendedproperty 'MS_Description', '大区考位单位维度增量数据', 'SCHEMA',
    'dbo', 'TABLE', 'ads_gp_oa_t_area_unit_sales_dd'
go
exec sp_addextendedproperty 'MS_Description', '年份', 'SCHEMA',
    'dbo', 'TABLE', 'ads_gp_oa_t_area_unit_sales_dd', 'COLUMN',
     'date_y'
go
exec sp_addextendedproperty 'MS_Description', '季度', 'SCHEMA',
    'dbo', 'TABLE', 'ads_gp_oa_t_area_unit_sales_dd', 'COLUMN',
     'date_q'
go
exec sp_addextendedproperty 'MS_Description', '月度', 'SCHEMA',
    'dbo', 'TABLE', 'ads_gp_oa_t_area_unit_sales_dd', 'COLUMN',
     'date_m'
go
exec sp_addextendedproperty 'MS_Description', '确认日期', 'SCHEMA',
    'dbo', 'TABLE', 'ads_gp_oa_t_area_unit_sales_dd', 'COLUMN',
     'confirm_date'
go
exec sp_addextendedproperty 'MS_Description', '大区', 'SCHEMA',
    'dbo', 'TABLE', 'ads_gp_oa_t_area_unit_sales_dd', 'COLUMN',
     'area'
go
exec sp_addextendedproperty 'MS_Description', '考核单位', 'SCHEMA',
    'dbo', 'TABLE', 'ads_gp_oa_t_area_unit_sales_dd', 'COLUMN',
     'area_unit'
go
exec sp_addextendedproperty 'MS_Description', '年度累计确认收入差值', 'SCHEMA',
    'dbo', 'TABLE', 'ads_gp_oa_t_area_unit_sales_dd', 'COLUMN',
     'income_diff'
go
exec sp_addextendedproperty 'MS_Description', '当前待确认收入差值', 'SCHEMA',
    'dbo', 'TABLE', 'ads_gp_oa_t_area_unit_sales_dd', 'COLUMN',
     'unincome_diff'
go