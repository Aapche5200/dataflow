from sqlalchemy import create_engine
import pandas as pd
import datetime
from pathlib import Path
import psycopg2 as ps
# import postgresql
# import nampy as np

# 1.连接数据库
# 连接基础信息平台数据库
sql_con_jichu = 'mssql+pymssql://sjfx:684qrRrdocTLugaeVQpD@10.20.121.228:1433/ecology'
engine_jichu = create_engine(sql_con_jichu)
# 连接erp型模表数据库
sql_con_xingmo = 'mssql+pymssql://sjfx:A20220307a*@192.168.3.85:50932/DBAPPSecurty'
engine_xingmo = create_engine(sql_con_xingmo)
# 连接自研报价器 数据库(正式)
sql_con_sdpm = 'postgresql://ltc_proconfig:gr4*#R7&Ej%*s#$6M3kY@10.20.121.113:32432/ltc_proconfig'
engine_sdpm = create_engine(sql_con_sdpm)
# 连接自研报价器 数据库(测试)
sql_con_sdpm_test = 'postgresql://postgres:kK9p&XiamtnO}03@10.20.120.140:32432/ltc_proconfig'
engine_sdpm_test = create_engine(sql_con_sdpm)

date = datetime.date.today().isoformat()[:10]
excel_dict = {'path': Path('..').joinpath('../导出数据')  # 默认excel路径
              }


def write2excel(df_dict, filename='temp', path=None, index=0):
    # 把df列表写入单个Excel文件
    if not path:
        path = excel_dict.get('path')
        if not path.exists():
            path.mkdir(parents=True, exist_ok=True)
    try:
        writer = pd.ExcelWriter(path / f'{filename}.xlsx')
    except PermissionError:
        time_str = time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()))
        writer = pd.ExcelWriter(path / f'{filename}_{time_str}.xlsx')

    # 如果是dataframe list的话，直接按照序号写入
    if isinstance(df_dict, list):
        for i, df in enumerate(df_dict):
            df.to_excel(writer, f'sheet{i}', index=index)

    # 如果是字典，字典的索引必须是字符串，字典的值是dataframe
    elif isinstance(df_dict, dict):
        for k, v in df_dict.items():
            v.to_excel(writer, k, index=index)

    # pd.Dataframe 直写文件
    elif isinstance(df_dict, pd.DataFrame):
        df_dict.to_excel(writer, index=index)
    else:
        print(type(df_dict), 'can not write')
    ## 保存文件
    writer.save()


# 2.产品型号信息一致性比对
date = datetime.date.today().isoformat()[:10]
# 取数并统一字段名称
sql_sdpm = '''
select tl.name 产品线,
tp.name 产品名称,
product_sale_name 产品销售名称,
special_version 销许型号,
tm.name 产品型号,
tm.id id,
tm.code 产品型号编码,
function_desc 功能描述,
--specification_config 规格说明,
hardware_env 规格说明,
config_guide 配置指导,
ext_desc 可扩展说明,
fit_user_type 适合用户类型,
fit_scene_desc 适用场景说明,
eom 停止销售,
eofs 停止全面支持,
eos 停止服务,
tm.category 行业渠道属性,
product_type 产品类型,
unit 计量单位,
is_secret 是否涉密,
is_itaii 是否信创,
is_business_secret 是否商密,
product_manager 产品经理,
product_manager_code 产品经理工号,
localization 国产化,
tm.status 生命周期状态
from t_model tm
left join t_product_line tl on tm.pl_id=tl.id
left join t_product  tp on tp.id=pd_id
where coa_status=1
'''
sql_xingmo = '''
--产品型号信息
select * from (select b2.name 产品线,
b1.name 产品名称,
b.DescFlexField_PrivateDescSeg7 产品销售名称,
b.Xxxh  销许型号,
b.name 产品型号,
b.code 产品型号编码,
b.GNMS 功能描述,
b.GG 规格说明,
b.BZ 配置指导,
b.DescFlexField_PrivateDescSeg6 可扩展说明,
b.SHYHLX 适合用户类型,
b.SYCJSM 适用场景说明,
b.EOM  停止销售,
b.EOFS 停止全面支持,
b.EOS  停止服务,
d.name 行业渠道属性,
b.CPLX 产品类型,
b.JLDW 计量单位,
b.ISSM 是否涉密,
b.ISXC 是否信创,
b.ISSHM 是否商密,
b.CPJL 产品经理,
b.DescFlexField_PrivateDescSeg5 产品经理工号,
b.SX 生效
from Cust_AHAPP_ProductModelInfo b
left join Cust_AHAPP_ProductModelInfo b1 on b.sjcode=b1.id
left join Cust_AHAPP_ProductModelInfo b2 on b1.sjcode=b2.id
left join (select c.name,b.* from UBF_Sys_ExtEnumType_Trl a 
left join UBF_Sys_ExtEnumValue b on a.ID=b.ExtEnumType 
left join UBF_Sys_ExtEnumValue_Trl c on b.ID=c.ID
where a.Name= '行业渠道属性')d on d.EValue=b.HYQDSX) as fm
where 产品线 is NOT NULL
and 生效=1
'''
dp_sdpm = pd.read_sql(sql_sdpm, engine_sdpm)
dp_sdpm['行业渠道属性'] = dp_sdpm['行业渠道属性'].map({10: '行业', 20: '渠道'})
dp_sdpm['产品类型'] = dp_sdpm['产品类型'].map({10: '嵌入式软件', 20: '软件', 30: '服务'})
dp_sdpm['是否涉密'] = dp_sdpm['是否涉密'].map({0: '否', 1: '是'})
dp_sdpm['是否信创'] = dp_sdpm['是否信创'].map({0: '否', 1: '是'})
dp_sdpm['是否商密'] = dp_sdpm['是否商密'].map({0: '否', 1: '是'})
dp_sdpm['id'] = dp_sdpm['id'].astype('str')
dp_xingmo = pd.read_sql(sql_xingmo, engine_xingmo)
dp_xingmo['是否涉密'] = dp_xingmo['是否涉密'].map({0: '否', 1: '是'})
dp_xingmo['是否信创'] = dp_xingmo['是否信创'].map({0: '否', 1: '是'})
dp_xingmo['是否商密'] = dp_xingmo['是否商密'].map({0: '否', 1: '是'})
# 确定需要比对的字段并连接数据
last_col = ['产品线', '产品名称', '产品销售名称', '销许型号', '产品型号编码', '功能描述',
            '配置指导', '可扩展说明', '适合用户类型', '适用场景说明', '停止销售', '停止全面支持', '停止服务',
            '行业渠道属性', '产品类型', '计量单位', '是否商密', '是否信创', '是否涉密', '产品经理', '产品经理工号']
df = dp_sdpm.merge(dp_xingmo, how='left', on='产品型号')
# 开始比对
dfn = []
dfs = {'统计': None, '产品型号信息_sdpm': dp_sdpm, '产品型号信息_xingmo': dp_xingmo}
dp_xingmo_change = pd.DataFrame()
dp_xingmo_change['类型'] = '产品型号信息'
dp_xingmo_change['产品型号'] = dp_sdpm['产品型号']
for col in last_col:
    df4 = df.loc[df[f'{col}_x'] != df[f'{col}_y'], ['产品型号', f'{col}_x', f'{col}_y']]
    dp_xingmo_change = dp_xingmo_change.merge(df4, how='left', on='产品型号')
    dfn.append([col, len(df4)])
    dfs[col] = df4
    # print(col)
dfs['统计'] = pd.DataFrame(dfn, columns=['字段', '异常数量'])
dp_xingmo_change_final = dp_xingmo_change.loc[:, ['序号', '类型', '产品型号',
                                                  '功能描述_x',
                                                  '配置指导_x',
                                                  '可扩展说明_x',
                                                  '适合用户类型_x',
                                                  '适用场景说明_x',
                                                  '停止销售_x',
                                                  '停止全面支持_x',
                                                  '停止服务_x',
                                                  '行业渠道属性_x',
                                                  '产品类型_x',
                                                  '计量单位_x',
                                                  '是否商密_x',
                                                  '是否信创_x',
                                                  '是否涉密_x',
                                                  '产品经理_x',
                                                  '产品经理工号_x']]
dp_xingmo_change_final['类型'] = '产品型号信息'
dp_xingmo_change_final.rename(columns={'功能描述_x': '功能描述',
                                       '配置指导_x': '配置指导',
                                       '可扩展说明_x': '可扩展说明',
                                       '适合用户类型_x': '适合用户类型',
                                       '适用场景说明_x': '适用场景说明',
                                       '停止销售_x': 'EOM',
                                       '停止全面支持_x': 'EOFS',
                                       '停止服务_x': 'EOS',
                                       '行业渠道属性_x': '行业渠道属性',
                                       '产品类型_x': '产品类型',
                                       '计量单位_x': '计量单位',
                                       '是否商密_x': '是否商密',
                                       '是否信创_x': '是否信创',
                                       '是否涉密_x': '是否涉密',
                                       '产品经理_x': '产品经理',
                                       '产品经理工号_x': '产品经理工号'}, inplace=True)

# 并行运维-型模表产品变更及新增模板
dp_xingmo_change_final = dp_xingmo_change_final.set_index('产品型号')
dp_xingmo_change_final['维护类型'] = ''
dp_xingmo_add = pd.DataFrame(
    columns=['生命周期状态', '产品线', '产品名称(销许名称)', '产品销售名称', '销许型号/版本', '产品型号', '产品版本号',
             '功能描述', '规格说明', '可扩展说明', '配置指导', '适合用户类型', '适用场景说明', 'EOM',
             'EOFS', 'EOS', '行业渠道属性', '产品类型', '计量单位', '是否涉密', '是否信创', '是否商密',
             '产品经理', '产品经理工号'])

for i in list(dp_xingmo_change_final.index):
    if i in list(dp_xingmo['产品型号']):
        dp_xingmo_change_final.loc[[f'{i}'], ['维护类型']] = '变更'
    else:
        dp_xingmo_change_final.loc[[f'{i}'], ['维护类型']] = '新增'
        dp_xingmo_add = dp_xingmo_add.append(dp_sdpm_add[(dp_sdpm_add.产品型号 == f'{i}')])
dp_xingmo_change_final2 = dp_xingmo_change_final[(dp_xingmo_change_final.维护类型 == '变更')]

dp_xingmo_add_final = pd.DataFrame(
    columns=['生命周期状态', '产品线', '产品名称(销许名称)', '产品销售名称', '销许型号/版本', '产品型号', '产品版本号',
             '功能描述', '规格说明', '可扩展说明', '配置指导', '适合用户类型', '适用场景说明', 'EOM',
             'EOFS', 'EOS', '行业渠道属性', '产品类型', '计量单位', '是否涉密', '是否信创', '是否商密',
             '产品经理', '产品经理工号'])
dp_xingmo_add_final['生命周期状态'] = dp_xingmo_add['生命周期状态']
dp_xingmo_add_final['产品线'] = dp_xingmo_add['产品线']
dp_xingmo_add_final['产品名称(销许名称)'] = dp_xingmo_add['产品名称(销许名称)']
dp_xingmo_add_final['产品销售名称'] = dp_xingmo_add['产品销售名称']
dp_xingmo_add_final['销许型号/版本'] = dp_xingmo_add['销许型号/版本']
dp_xingmo_add_final['产品型号'] = dp_xingmo_add['产品型号']
dp_xingmo_add_final['产品版本号'] = dp_xingmo_add['产品版本号']
dp_xingmo_add_final['功能描述'] = dp_xingmo_add['功能描述']
dp_xingmo_add_final['规格说明'] = dp_xingmo_add['规格说明']
dp_xingmo_add_final['可扩展说明'] = dp_xingmo_add['可扩展说明']
dp_xingmo_add_final['配置指导'] = dp_xingmo_add['配置指导']
dp_xingmo_add_final['适合用户类型'] = dp_xingmo_add['适合用户类型']
dp_xingmo_add_final['适用场景说明'] = dp_xingmo_add['适用场景说明']
dp_xingmo_add_final['EOM'] = dp_xingmo_add['EOM']
dp_xingmo_add_final['EOFS'] = dp_xingmo_add['EOFS']
dp_xingmo_add_final['EOS'] = dp_xingmo_add['EOS']
dp_xingmo_add_final['行业渠道属性'] = dp_xingmo_add['行业渠道属性']
dp_xingmo_add_final['产品类型'] = dp_xingmo_add['产品类型']
dp_xingmo_add_final['计量单位'] = dp_xingmo_add['计量单位']
dp_xingmo_add_final['是否涉密'] = dp_xingmo_add['是否涉密']
dp_xingmo_add_final['是否信创'] = dp_xingmo_add['是否信创']
dp_xingmo_add_final['是否商密'] = dp_xingmo_add['是否商密']
dp_xingmo_add_final['产品经理'] = dp_xingmo_add['产品经理']
dp_xingmo_add_final['产品经理工号'] = dp_xingmo_add['产品经理工号']
dp_xingmo_add_final2 = dp_xingmo_add_final[
    (dp_xingmo_add_final.生命周期状态 == '量产') | (dp_xingmo_add_final.生命周期状态 == '初始')]
write2excel(dp_xingmo_change_final2, f'产品型号变更导入模板_{date}')
write2excel(dp_xingmo_add_final2, f'产品型号新增导入模板_{date}')
write2excel(dfs, f'产品型号信息比对_{date}')
print('done')

# 3.产品模块信息一致性比对
date = datetime.date.today().isoformat()[:10]
# 取数并统一字段名称
sql_sdpm_mokuai = '''select name 产品模块名称,
material_code 物料编码,
function_desc 功能描述,
specification_desc 规格说明,
type_value 模块类型,
unit 计量单位,
is_oem_in 是否贴标,
category 行业渠道属性
from t_module
'''
sql_xingmo_mokuai = '''
select 
b.DescFlexField_PrivateDescSeg3  产品模块名称,
ci.code  物料编码,
b.MKGNMS  功能描述,
b.GGPZSM  规格说明,
b.XSDW  计量单位,
d.name  模块类型,
b.ISOEM  是否贴标
from Cust_AHAPP_ProductModularInfo b
left join CBO_ItemMaster ci on b.CPMK=ci.id
left join Cust_AHAPP_ProductModelInfo  cp on b.CPXH=cp.id
left join (select c.name,b.* from UBF_Sys_ExtEnumType_Trl a 
left join UBF_Sys_ExtEnumValue b on a.ID=b.ExtEnumType 
left join UBF_Sys_ExtEnumValue_Trl c on b.ID=c.ID
where a.Name= '模块类型')d on d.EValue=b.MKLX
where b.sx=1
'''
dp_sdpm_mokuai = pd.read_sql(sql_sdpm_mokuai, engine_sdpm)
dp_xingmo_mokuai = pd.read_sql(sql_xingmo_mokuai, engine_xingmo)
dp_xingmo_mokuai_2 = dp_xingmo_mokuai.drop_duplicates('物料编码', keep='first')
# 确定需要比对的字段并连接数据
last_col = ['产品模块名称', '功能描述', '规格说明', '模块类型', '计量单位', '是否贴标']
df = dp_xingmo_mokuai_2.merge(dp_sdpm_mokuai, how='left', on='物料编码')
# 开始比对
dfn = []
dfs = {'统计': None, '模块信息_sdpm': dp_sdpm_mokuai, '模块信息_xingmo': dp_xingmo_mokuai}
for col in last_col:
    df4 = df.loc[df[f'{col}_x'] != df[f'{col}_y'], ['物料编码', '产品模块名称', f'{col}_x', f'{col}_y']]
    dfn.append([col, len(df4)])
    dfs[col] = df4
    # print(col)
dfs['统计'] = pd.DataFrame(dfn, columns=['字段', '异常数量'])
write2excel(dfs, f'产品模块信息比对_{date}')
print('done')

# 4.价格信息一致性比对
date = datetime.date.today().isoformat()[:10]
# 取数并统一字段名称
sql_sdpm_price = '''select 
tp.id 系统id,
tp.status 状态,
tme.code 产品型号编码,
tme.name 产品型号,
tmu.name  产品模块,
tmu.material_code 物料编码,
tpt.name 价格名称,
tp.price 价格,
tp.default_price 默认价格,
tp.approve_status 审批状态,
tp.module_type 模块类型,
tmu.category 行业渠道属性,
tp.ladder_price 阶梯价
from t_price tp
left join t_model  tme on tp.model_id=tme.id
left join t_module tmu on tp.module_id=tmu.id
left join t_price_type  tpt on tpt.id=tp.pt_id
where tmu.name not in ('默认含质保期限（1年）','默认含质保期限（38个月）','默认含质保期限（3年）','默认含质保期限（42个月）')
and tp.module_type not in ('70','90','100')  --70:'通用硬件配件' 90：'默认维保',100:'维保增购'
'''

sql_xingmo_price = '''
select
c.name 产品型号,
cp.DescFlexField_PrivateDescSeg3  产品模块,
e.name 模块类型,
cp.MKXZ  模块选择,
ci.code 物料编码,
b.PriceType  价格类型,
b.PriceName  价格名称,
b.PriceMny  价格
from Cust_AHAPP_PriceDetail b
left join Cust_AHAPP_ProductModularInfo cp on b.ProductModularInfo=cp.id
left join CBO_ItemMaster ci on cp.cpmk = ci.id
left join Cust_AHAPP_ProductModelInfo c on cp.CPXH=c.id
left join (select c.name,b.* from UBF_Sys_ExtEnumType_Trl a 
left join UBF_Sys_ExtEnumValue b on a.ID=b.ExtEnumType 
left join UBF_Sys_ExtEnumValue_Trl c on b.ID=c.ID
where a.Name= '模块选择')d on d.EValue=cp.MKXZ
left join (select c.name,b.* from UBF_Sys_ExtEnumType_Trl a 
left join UBF_Sys_ExtEnumValue b on a.ID=b.ExtEnumType 
left join UBF_Sys_ExtEnumValue_Trl c on b.ID=c.ID
where a.Name= '模块类型')e on e.EValue=cp.MKLX 
where cp.SX=1
'''
dp_sdpm_price = pd.read_sql(sql_sdpm_price, engine_sdpm)
dp_xingmo_price = pd.read_sql(sql_xingmo_price, engine_xingmo)
dp_sdpm_price['物料编码'] = dp_sdpm_price['物料编码'].astype('str')
dp_sdpm_price['产品型号'] = dp_sdpm_price['产品型号'].astype('str')
dp_sdpm_price['价格名称'] = dp_sdpm_price['价格名称'].astype('str')
dp_sdpm_price['系统id'] = dp_sdpm_price['系统id'].astype('str')
dp_xingmo_price['物料编码'] = dp_xingmo_price['物料编码'].astype('str')
dp_xingmo_price['产品型号'] = dp_xingmo_price['产品型号'].astype('str')
dp_xingmo_price['价格名称'] = dp_xingmo_price['价格名称'].astype('str')
dp_sdpm_price['id'] = dp_sdpm_price.apply(lambda x: x['产品型号'] + x['物料编码'] + x['价格名称'], axis=1)
dp_xingmo_price['id'] = dp_xingmo_price.apply(lambda x: x['产品型号'] + x['物料编码'] + x['价格名称'], axis=1)
dp_sdpm_price['审批状态'] = dp_sdpm_price['审批状态'].map({10: '未配置', 20: '待审批', 30: '审批拒绝', 40: '审批通过'})
dp_sdpm_price['模块类型'] = dp_sdpm_price['模块类型'].map(
    {10: '软硬一体', 20: '软件', 30: '服务', 40: '许可（受年限约束）', 50: '许可（不受年限约束）', 60: '定制开发',
     80: '硬件配件'})
dp_sdpm_price['行业渠道属性'] = dp_sdpm_price['行业渠道属性'].map({10: '行业', 20: '渠道'})
# 确定需要比对的字段并连接数据
last_col = ['价格']
df = dp_sdpm_price.merge(dp_xingmo_price, how='left', on='id')
# 开始比对
dfn = []
dfs = {'统计': None, '价格信息_sdpm': dp_sdpm_price, '价格信息_xingmo': dp_xingmo_price}
for col in last_col:
    df4 = df.loc[df[f'{col}_x'] != df[f'{col}_y'], ['id', f'{col}_x', f'{col}_y']]
    dfn.append([col, len(df4)])
    dfs[col] = df4
    # print(col)
dfs['统计'] = pd.DataFrame(dfn, columns=['字段', '异常数量'])
write2excel(dfs, f'产品价格信息比对_{date}')
print('done')

# 5.SBOM信息一致性比对
date = datetime.date.today().isoformat()[:10]
# 取数并统一字段名称
sql_sdpm = '''
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
sql_xingmo = '''
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
dp_sdpm_sbom = pd.read_sql(sql_sdpm, engine_sdpm)
dp_xingmo_sbom = pd.read_sql(sql_xingmo, engine_xingmo)
# dp_sdpm['物料编码']=dp_sdpm['物料编码'].map({pd.isnull:'空'})
# dp_xingmo['物料编码']=dp_xingmo['物料编码'].map({pd.isnull:'空'})
dp_sdpm_sbom['物料编码'] = dp_sdpm_sbom['物料编码'].astype('str')
dp_sdpm_sbom['产品型号编码'] = dp_sdpm_sbom['产品型号编码'].astype('str')
dp_sdpm_sbom['id'] = dp_sdpm_sbom['id'].astype('str')
dp_xingmo_sbom['物料编码'] = dp_xingmo_sbom['物料编码'].astype('str')
dp_xingmo_sbom['产品型号编码'] = dp_xingmo_sbom['产品型号编码'].astype('str')
dp_xingmo_sbom['模块选择'] = dp_xingmo_sbom['模块选择'].map({1: '必选', 2: '可选', 3: '推荐', 4: '可选', 5: '替换'})
dp_xingmo_sbom['模块配置规则'] = dp_xingmo_sbom['模块配置规则'].map({-1: '空', 0: '空',
                                                                     6: '合计不超过A组', 7: '合计不超过B组',
                                                                     8: '合计不超过C组',
                                                                     3: 'N选择1C组', 2: 'N选择1B组', 1: 'N选择1A组'})
dp_sdpm_sbom['型模列'] = dp_sdpm_sbom.apply(lambda x: x['产品型号编码'] + x['物料编码'], axis=1)
dp_xingmo_sbom['型模列'] = dp_xingmo_sbom.apply(lambda x: x['产品型号编码'] + x['物料编码'], axis=1)
# 确定需要比对的字段并连接数据
last_col = ['模块选择',
            '模块最大数量', '模块最小数量', '默认数量', '模块配置规则', '模块数量合计不超过', '配置指导']
df = dp_sdpm_sbom.merge(dp_xingmo_sbom, how='left', on='型模列')
# 开始比对
dfn = []
dp_xingmo_change_sbom = pd.DataFrame()
dp_xingmo_change_sbom['型模列'] = dp_sdpm_sbom['型模列']
dfs = {'统计': None, '模块信息_sdpm': dp_sdpm_sbom, '模块信息_xingmo': dp_xingmo_sbom}
for col in last_col:
    df4 = df.loc[df[f'{col}_x'] != df[f'{col}_y'], ['型模列', f'{col}_x', f'{col}_y']]
    dfn.append([col, len(df4)])
    dp_xingmo_change_sbom = dp_xingmo_change_sbom.merge(df4, how='left', on='型模列')
    dfs[col] = df4
    # print(col)
dfs['统计'] = pd.DataFrame(dfn, columns=['字段', '异常数量'])

# SBOM变更
dp_xingmo_change_sbom['类型'] = '产品模块信息'
dp_xingmo_change_sbom['序号'] = ''
dp_xingmo_change_sbom2 = dp_xingmo_change_sbom.loc[:, ['类型', '型模列', '模块选择_x', '模块最大数量_x',
                                                       '模块最小数量_x', '默认数量_x', '模块配置规则_x',
                                                       '模块数量合计不超过_x',
                                                       '配置指导_x', '序号']]
dp_xingmo_change_sbom2 = dp_xingmo_change_sbom2.rename(
    columns={'模块选择_x': '模块选择', '模块最大数量_x': '模块最大数量',
             '模块最小数量_x': '模块最小数量', '默认数量_x': '默认数量', '模块配置规则_x': '模块配置规则',
             '模块数量合计不超过_x': '模块数量合计不超过', '配置指导_x': '配置指导'})
dp_xingmo_change_sbom3 = dp_xingmo_change_sbom2[
    (dp_xingmo_change_sbom2['模块选择'].notna()) | (dp_xingmo_change_sbom2['模块最大数量'].notna())
    | (dp_xingmo_change_sbom2['模块最小数量'].notna()) | (dp_xingmo_change_sbom2['默认数量'].notna())
    | (dp_xingmo_change_sbom2['模块配置规则'].notna())
    | (dp_xingmo_change_sbom2['模块数量合计不超过'].notna())
    | (dp_xingmo_change_sbom2['配置指导'].notna())]
dp_xingmo_change_sbom4 = dp_sdpm.loc[:, ['型模列', '产品型号', '产品模块', '物料编码']]
dp_xingmo_change_sbom5 = dp_sdpm_mokuai.loc[:, ['物料编码', '功能描述', '规格说明', '模块类型', '计量单位', '']]
dp_xingmo_change_sbom6 = dp_xingmo_change_sbom3.merge(dp_xingmo_change_sbom4, how='left', on='型模列')
dp_xingmo_change_sbom7 = dp_xingmo_change_sbom6.set_index('型模列')
dp_xingmo_change_sbom7['维护类型'] = ''
for i in list(dp_xingmo_change_sbom7.index):
    if i not in list(dp_xingmo_sbom['型模列']):
        dp_xingmo_change_sbom7.loc[[f'{i}'], ['维护类型']] = '新增'
    else:
        dp_xingmo_change_sbom7.loc[[f'{i}'], ['维护类型']] = '变更'
dp_xingmo_change_sbom8 = dp_xingmo_change_sbom7[(dp_xingmo_change_sbom7.维护类型 == '变更')]
dp_xingmo_change_sbom_final = dp_xingmo_change_sbom8.loc[:, ['序号', '类型', '产品型号', '产品模块', '模块选择',
                                                             '模块最大数量', '模块最小数量', '默认数量', '模块配置规则',
                                                             '模块数量合计不超过', '配置指导', '维护类型']].rename(
    columns={'产品型号': '原产品型号',
             '产品模块': '原产品模块名称'})

# SBOM新增
dp_xingmo_sbom_add = dp_xingmo_change_sbom7[(dp_xingmo_change_sbom7.维护类型 == '新增')]
dp_sdpm_mokuai2 = dp_sdpm_mokuai.loc[:,
                  ['物料编码', '功能描述', '规格说明', '模块类型', '计量单位', '是否贴标']].rename(
    columns={'是否贴标': '是否OEM IN'})
dp_sdpm_price1 = dp_sdpm_price.loc[:, ['产品型号', '产品模块', '物料编码', '行业渠道属性', '价格名称', '价格']].rename(
    columns={'行业渠道属性': '价格类型'})
dp_sdpm_price1['型模列'] = dp_sdpm_price1.apply(lambda x: x['产品型号'] + x['物料编码'], axis=1)
dp_sdpm_price3 = dp_sdpm_price1.loc[:, ['型模列', '价格类型', '价格名称', '价格']]
# dp_xingmo_sbom_add=pd.DataFrame(columns=['型模列','产品型号','产品模块名称','产品模块','模块选择','模块最大数量','模块最小数量',
# '默认数量','模块配置规则','模块数量合计不超过','配置指导'])
dp_xingmo_sbom_add2 = dp_xingmo_sbom_add.loc[:, ['维护类型', '产品型号', '产品模块', '物料编码',
                                                 '模块选择', '模块最大数量', '模块最小数量', '默认数量',
                                                 '模块配置规则', '模块数量合计不超过', '配置指导']]

dp_xingmo_sbom_add3 = dp_xingmo_sbom_add2.merge(dp_sdpm_mokuai2, how='left', on='物料编码')
dp_xingmo_sbom_add3['型模列'] = dp_xingmo_sbom_add3.apply(lambda x: x['产品型号'] + x['物料编码'], axis=1)
dp_xingmo_sbom_add4 = dp_xingmo_sbom_add3.merge(dp_sdpm_price3, how='left', on='型模列')
dp_xingmo_sbom_add4.info()

write2excel(dp_xingmo_sbom_add2, f'SBOM新增_{date}')
write2excel(dfs, f'SBOM信息比对_{date}')
write2excel(dp_xingmo_change_sbom_final, f'SBOM变更_{date}')
print('done')

dp_sdpm_mokuai.info()

dp_sdpm_mokuai2 = dp_sdpm_mokuai.loc[:,
                  ['物料编码', '功能描述', '规格说明', '模块类型', '计量单位', '是否贴标']].rename(
    columns={'是否贴标': '是否OEM IN'})
dp_sdpm_mokuai2.info()

dp_sdpm_price.info()

dp_xingmo_sbom_add2.info()

# SBOM新增
dp_xingmo_sbom_add = dp_xingmo_change_sbom7[(dp_xingmo_change_sbom7.维护类型 == '新增')]
dp_sdpm_mokuai2 = dp_sdpm_mokuai.loc[:,
                  ['物料编码', '功能描述', '规格说明', '模块类型', '计量单位', '是否贴标']].rename(
    columns={'是否贴标': '是否OEM IN'})
dp_sdpm_price1 = dp_sdpm_price.loc[:, ['产品型号', '产品模块', '物料编码', '行业渠道属性', '价格名称', '价格']].rename(
    columns={'行业渠道属性': '价格类型'})
dp_sdpm_price1['型模列'] = dp_sdpm_price1.apply(lambda x: x['产品型号'] + x['物料编码'], axis=1)
dp_sdpm_price3 = dp_sdpm_price1.loc[:, ['型模列', '价格类型', '价格名称', '价格']]
# dp_xingmo_sbom_add=pd.DataFrame(columns=['型模列','产品型号','产品模块名称','产品模块','模块选择','模块最大数量','模块最小数量',
# '默认数量','模块配置规则','模块数量合计不超过','配置指导'])
dp_xingmo_sbom_add2 = dp_xingmo_sbom_add.loc[:, ['维护类型', '产品型号', '产品模块', '物料编码',
                                                 '模块选择', '模块最大数量', '模块最小数量', '默认数量',
                                                 '模块配置规则', '模块数量合计不超过', '配置指导']]

dp_xingmo_sbom_add3 = dp_xingmo_sbom_add2.merge(dp_sdpm_mokuai2, how='left', on='物料编码')
dp_xingmo_sbom_add3['型模列'] = dp_xingmo_sbom_add3.apply(lambda x: x['产品型号'] + x['物料编码'], axis=1)
dp_xingmo_sbom_add4 = dp_xingmo_sbom_add3.merge(dp_sdpm_price3, how='left', on='型模列')
dp_xingmo_sbom_add4.info()

dp_xingmo_sbom_add4.head()

dp_xingmo_change['类型'] = '产品型号信息'
dp_xingmo_change['产品型号'] = dp_sdpm['产品型号']
dp_xingmo_change.head()

del dp_xingmo_change['原产品型号']

dp_xingmo_change.head()
