import pandas as pd
import numpy as np
import re

common_field = [
    '产品线',
    '产品名称',
    '产品型号'
]

changed_arrange = [
    # '许可说明',
    '配置指导',
    # '行业渠道属性',
    # '适合用户类型',
    # '适用场景说明',
    # '是否涉密',
    # '是否信创',
    # '是否商密',
    # '可扩展说明',
    # '停止销售EOM',
    # '停止全面支持EOFS',
    # '停止服务EOS',
    # '销许型号',
    # '计量单位'
]

# 读取报价器数据库SBOM表
data_quoter = pd.read_excel(r'C:/Users/allan.yin/PycharmProjects/pythonProject/ah/compare/陈硕.xlsx',
                            usecols=common_field + changed_arrange + ['是否型号'])
data_quoter = data_quoter[data_quoter['是否型号'] == '是']
data_quoter = data_quoter.drop_duplicates()

# 读取erp数据库sbom表
data_erp = pd.read_excel(r'C:/Users/allan.yin/PycharmProjects/pythonProject/ah/compare/滕冀4.xlsx',
                         usecols=common_field + changed_arrange)
data_erp = data_erp.drop_duplicates()

# 定义正则表达式规则
pattern = r'^[\s.,。\\/]+$'


# 定义函数进行处理
def process_value(value):
    if re.match(pattern, str(value)):
        return None  # 将满足正则表达式的值置为空
    return value  # 不满足条件的值保持不变


data_quoter = data_quoter.applymap(process_value)
data_erp = data_erp.applymap(process_value)

# 报价器和erp 空值数据处理
for all_col in changed_arrange:
    data_erp[all_col].fillna('', inplace=True)
    data_erp[all_col] = data_erp[all_col].replace('', None)
    if all_col in data_quoter.columns:
        data_quoter[all_col].fillna('', inplace=True)
        data_quoter[all_col] = data_quoter[all_col].replace('', None)
    # 处理空值
    # data_quoter[all_col].replace({np.nan: 'EMPTY'}, inplace=True)
    # data_erp[all_col].replace({np.nan: 'EMPTY'}, inplace=True)

# 按照型模合并新的数据集
data_changed = data_quoter.merge(data_erp, on='产品型号', how='inner')

for column in changed_arrange:
    conditions = [
        data_changed[column + '_x'] != data_changed[column + '_y'],  # 列与对应的列进行比较
    ]

    conditions_null = [data_changed[column + '_y'].isnull()]

    choices = [
        data_changed[column + '_x'],  # 如果条件为真，将列的值赋给新列
    ]

    data_changed[column + '_changed'] = np.select(conditions, choices, default=None)

# 创建变更字段结果表格
common_field_y = [itemy + '_y' for itemy in common_field]
common_field_changed = [item_common + '_y' for item_common in changed_arrange]
changed_arrange_changed = [itemchanged + '_changed' for itemchanged in changed_arrange]
changed_rows_table = data_changed.filter(
    items=common_field_y + common_field_changed + ['产品型号'] + changed_arrange_changed)

# '产品生命周期状态_changed',
# '产品类型'
# '型号类别_changed',
# '计量单位中文名称_changed'
# 过滤对比的列都为空的行
changed_rows_filtered = changed_rows_table.dropna(subset=changed_arrange_changed, how='all')
changed_rows_filtered.drop_duplicates()

changed_rows_filtered.to_excel('C:/Users/allan.yin/PycharmProjects/pythonProject/ah/compare/changed_fields_table.xlsx',
                               index=False)
