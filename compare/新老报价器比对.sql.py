import pandas as pd
import numpy as np
import re

common_field = [
    '产品线',
    '产品名称',
    '产品型号'
]

changed_arrange = [
    '产品类型',
    '功能描述',
    '产品生命周期状态',
    '停止销售EOM',
    '停止全面支持EOFS',
    '停止服务EOS',
    '行业渠道属性',
    '适合用户类型',
    '适用场景说明',
    '计量单位',
    '是否涉密',
    '是否信创',
    '是否商密',
    '销许型号',
    '型号类别',
    '产品销售名称',
    '产品经理',
    '产品总监'

]

# 读取erp数据库sbom表
data_erp = pd.read_excel(r'C:/Users/allan.yin/PycharmProjects/pythonProject/ah/compare/滕冀5.xlsx',
                         usecols=common_field + changed_arrange)
data_erp = data_erp.drop_duplicates()

# 定义正则表达式规则
pattern = r'^[\s.,。\\/]+$'


# 定义函数进行处理
def process_value(value):
    if re.match(pattern, str(value)):
        return None  # 将满足正则表达式的值置为空
    return value  # 不满足条件的值保持不变


data_erp = data_erp.applymap(process_value)

# 报价器和erp 空值数据处理
for all_col in changed_arrange:
    data_erp[all_col].fillna('', inplace=True)
    data_erp[all_col] = data_erp[all_col].replace('', None)

# 按照型模合并新的数据集
data_changed = data_erp

for column in changed_arrange:
    conditions_null = [data_changed[column].isnull()]

    choices = [
        data_changed[column],  # 如果条件为真，将列的值赋给新列
    ]

    data_changed[column + '_changed'] = np.select(conditions_null, choices, default=None)

changed_arrange_changed = [itemchanged + '_changed' for itemchanged in changed_arrange]
changed_rows_table = data_changed.filter(
    items=common_field + changed_arrange_changed)

changed_rows_table.drop_duplicates()

changed_rows_table.to_excel('C:/Users/allan.yin/PycharmProjects/pythonProject/ah/compare/changed_fields_table.xlsx',
                            index=False)
