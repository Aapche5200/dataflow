import re
from utils.read_sql_ck import read_ck_df


def report_sql_transform(sql, param_dict):
    names = locals()
    for k, v in param_dict.items():
        names[k] = v

    # 处理变量
    express_list = re.findall('\$\{[0-9a-zA-Z_]*?\}', sql)
    for express in express_list:
        express_str = eval(express.replace(r'${', '').replace(r'}', ''))
        sql = sql.replace(express, express_str)
    # 处理 if 表达式
    express_list = re.findall('\$\{if.*?,.*?\}', sql)
    for express in express_list:
        express2 = express.replace(r'${if(', '').replace(r')}', '')
        express_detail = express2.split(',')
        if len(express_detail) > 3:
            express_detail = express_detail[:2] + [','.join(express_detail[2:])]
        if eval(express_detail[0].replace('=', '==')):
            express_str = eval(express_detail[1])
        else:
            express_str = eval(express_detail[2])
        sql = sql.replace(express, express_str)
    return sql


def test_reprot_sql(sql, param_dict):
    sql = report_sql_transform(sql, param_dict)
    print(sql)
    df = read_ck_df(sql)
    print(df)
    return df

# ${if(len(author)=0,"  "," and author ='"+ author + "' ")}
# ${if(len(file_name)=0,"  "," and file_name in ('"+ file_name + "') ")}
# where t2.log_date between '${begin_date}' and '${end_date}'