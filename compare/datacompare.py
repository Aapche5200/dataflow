import pandas as pd
import numpy as np
from ah.out_data.parket.db_con import DbCon as db_con

db_quoter = db_con.con_quoter
db_erp = db_con.con_erp

# 单独取模块信息
sql_quoter_module = '''
select name 产品模块名称,
material_code 物料编码,
function_desc 功能描述,
specification_desc 规格说明,
type_value 模块类型,
unit 计量单位,
is_oem_in 是否OEMIN,
category 行业渠道属性
from t_module
'''

data_quoter_module = pd.read_sql(sql_quoter_module, db_quoter)
print(data_quoter_module.head(10))

# 读取报价器数据库SBOM表
sql_quoter = '''
select 
trc.id id,
tme.code 产品型号编码,
tme.name 产品型号,
tmu.name  产品模块,
tmu.material_code 物料编码,
choice_value 模块选择,
max_count 模块最大数量,
min_count 模块最小数量,
default_count 默认数量,
rule_value 模块配置规则,
total_count 模块数量合计不超过,
trc.config_guide 配置指导
from t_rule_config trc
left join t_model  tme on trc.model_id=tme.id
left join t_module tmu on trc.module_id=tmu.id
where tmu.name not in('默认含质保期限（1年）','默认含质保期限（38个月）','默认含质保期限（3年）','默认含质保期限（42个月）')
'''
data_quoter = pd.read_sql(sql_quoter, db_quoter)

# 增加报价器数据库SBOM表型模列
data_quoter['change_编码_编码'] = data_quoter['产品型号编码'] + data_quoter['物料编码']
data_quoter['new_型号_编码'] = data_quoter['产品型号'] + data_quoter['物料编码']

# 读取erp数据库sbom表
erp_sql = '''
select 
cp.name  产品型号,
cp.code  产品型号编码,
b.DescFlexField_PrivateDescSeg3  产品模块,
ci.code  物料编码,
b.MKXZ  模块选择,
b.ZXSL  模块最小数量,
b.ZDSL  模块最大数量,
b.MRSL  默认数量,
b.THGZ  模块配置规则,
b.HJBCGSL  模块数量合计不超过,
b.BZ  配置指导
from Cust_AHAPP_ProductModularInfo b
left join CBO_ItemMaster ci on b.CPMK=ci.id
left join Cust_AHAPP_ProductModelInfo  cp on b.CPXH=cp.id
left join (select c.name,b.* from UBF_Sys_ExtEnumType_Trl a 
left join UBF_Sys_ExtEnumValue b on a.ID=b.ExtEnumType 
left join UBF_Sys_ExtEnumValue_Trl c on b.ID=c.ID
where a.Name= '模块类型')d on d.EValue=b.MKLX
where b.sx=1
'''
data_erp = pd.read_sql(erp_sql, db_erp)

# erp数据转换
data_erp['模块选择'] = data_erp['模块选择'].map({1: '必选', 2: '可选', 3: '推荐', 4: '可选', 5: '替换'})

data_erp['模块配置规则'] = data_erp['模块配置规则'].map({-1: None, 0: None, 6: '合计不超过A组',
                                                         7: '合计不超过B组', 8: '合计不超过C组',
                                                         3: 'N选择1C组', 2: 'N选择1B组', 1: 'N选择1A组'})
# 增加erp数据库SBOM表型模列
data_erp['change_编码_编码'] = data_erp['产品型号编码'] + data_erp['物料编码']
data_erp['new_型号_编码'] = data_erp['产品型号'] + data_erp['物料编码']

changed_arrange = ['模块选择', '模块配置规则', '配置指导',
                   '模块最大数量', '模块最小数量', '默认数量', '模块数量合计不超过']

# 报价器和erp 空值数据处理
for all_col in changed_arrange:
    data_quoter[all_col].fillna('', inplace=True)
    data_erp[all_col].fillna('', inplace=True)
    data_quoter[all_col] = data_quoter[all_col].replace('', None)
    data_erp[all_col] = data_erp[all_col].replace('', None)
    # 处理空值
    # data_quoter[all_col].replace({np.nan: 'EMPTY'}, inplace=True)
    # data_erp[all_col].replace({np.nan: 'EMPTY'}, inplace=True)

# 创建一个表格
data_excel_writer = \
    pd.ExcelWriter('C:/Users/allan.yin/PycharmProjects/pythonProject/ah/compare/比对结果.xlsx', engine='xlsxwriter')

data_quoter.to_excel(data_excel_writer, 'data_quoter', index=False)
data_erp.to_excel(data_excel_writer, 'data_erp', index=False)

# 比对报价器数据库SBOM表和erp数据库sbom表的[物料编码]字段
data_quoter_a_values = set(data_quoter['change_编码_编码'])
data_erp_a_values = set(data_erp['change_编码_编码'])

# 查找新增数值
new_values = data_quoter_a_values - data_erp_a_values

# 创建新增数值结果表格
if new_values:
    new_values_table = data_quoter[data_quoter['change_编码_编码'].isin(new_values)]
    new_values_table = new_values_table.merge()
    new_values_table.to_excel(data_excel_writer, 'new_values_table', index=False)

# 对报价器和erp索引进行重置
data_quoter.reset_index(drop=True, inplace=True)
data_erp.reset_index(drop=True, inplace=True)

# 比较列，查找变更
changed_rows = pd.DataFrame()

# 按照型模合并新的数据集
data_changed = data_quoter.merge(data_erp, on='change_编码_编码', how='inner')

# 遍历相应的列进行比较
for column in changed_arrange:
    if column in data_erp.columns:
        data_quoter_column_values = data_changed[column + '_x']
        data_erp_column_values = data_changed[column + '_y']

        # 判断报价器表的字段是否为空
        is_changed = data_quoter_column_values != data_erp_column_values
        not_empty = data_quoter_column_values.notnull()
        # 过滤列：如果发生变化，取出变化值；如果未发生变化，置为空值取出；
        # 如果报价器本身为空值, 比较时为：变化，取出也是空，则当做空值处理，如果这行7个字段都为空，则过滤视为未发生变化
        changed_values = np.where(is_changed, data_quoter_column_values, np.nan)
        # 过滤出有变化的行
        changed_rows = changed_rows.append(
            data_changed[is_changed | not_empty].assign(**{column + '_changed': changed_values}))

changed_rows = changed_rows.drop_duplicates()

# 创建变更字段结果表格
changed_rows_table = changed_rows.filter(items=[
    '产品型号_x',
    '产品模块_x',
    '模块选择_changed',
    '模块最大数量_changed',
    '模块最小数量_changed',
    '默认数量_changed',
    '模块配置规则_changed',
    '模块数量合计不超过_changed',
    '配置指导_changed'])

# 过滤对比的列都为空的行
changed_rows_x = [x + '_changed' for x in changed_arrange]
changed_rows_filtered = changed_rows_table.dropna(subset=changed_rows_x, how='all')

changed_rows_filtered.drop_duplicates()
changed_rows_filtered.to_excel(data_excel_writer, 'changed_fields_table', index=False)

# 保存excel
data_excel_writer.save()
data_excel_writer.close()
