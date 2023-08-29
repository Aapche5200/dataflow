import re
from ah.website.db_con import DbCon as db_con

db_gp = db_con.con_gp


def parse_source_target_tables(sql_query):
    source_tables = set()
    target_tables = set()

    # 提取'FROM'和'JOIN'之后的表
    from_tables = re.findall(r'\bFROM\s+([\w\.]+)', sql_query, re.IGNORECASE)
    join_tables = re.findall(r'\bJOIN\s+([\w\.]+)', sql_query, re.IGNORECASE)

    # 剔除从“from”子句中的子查询中筛选出表
    from_tables = [table for table in from_tables if '.' not in table]

    # 剔除筛选出名称中包含“temp”的表
    from_tables = [table for table in from_tables if 'temp' not in table.lower()]
    join_tables = [table for table in join_tables if 'temp' not in table.lower()]

    source_tables.update(from_tables)
    source_tables.update(join_tables)

    # 提取“UPDATE”和“INSERT”之后的表
    update_tables = re.findall(r'\bUPDATE\s+([\w\.]+)', sql_query, re.IGNORECASE)
    insert_table = re.findall(r'\bINSERT\s+INTO\s+([\w\.]+)', sql_query, re.IGNORECASE)

    # 剔除筛选出名称中包含“temp”的表
    update_tables = [table for table in update_tables if 'temp' not in table.lower()]
    insert_table = [table for table in insert_table if 'temp' not in table.lower()]

    target_tables.update(update_tables)
    target_tables.update(insert_table)

    # 剔除筛选出SQL函数
    sql_functions = re.findall(r'\b([A-Za-z_][A-Za-z0-9_]*)\(', sql_query)
    source_tables = {table for table in source_tables if table not in sql_functions}
    target_tables = {table for table in target_tables if table not in sql_functions}

    # 剔除筛选出WITH子句中的表
    with_tables = re.findall(r'\bWITH\s+([\w, ]+)\s+AS', sql_query, re.IGNORECASE)
    with_tables = [tbl.strip() for tbl in re.findall(r'(\w+)\s+AS', ','.join(with_tables))]
    source_tables = {table for table in source_tables if not any(table.startswith(tbl) for tbl in with_tables)}
    target_tables = {table for table in target_tables if not any(table.startswith(tbl) for tbl in with_tables)}

    # 剔除筛选出以“temp”开头的表
    source_tables = {table for table in source_tables if not table.startswith('temp')}
    target_tables = {table for table in target_tables if not table.startswith('temp')}

    return source_tables, target_tables

# 从数据库查找表，用来判断解析出来的表，是否这这里面
query_showtable = f"""SELECT
                    relname AS table_name
                FROM  pg_class c
                JOIN pg_roles r ON c.relowner = r.oid
                WHERE
                    relkind = 'r'
                    AND relname NOT LIKE 'pg_%%'
                    AND relname NOT LIKE 'information_%%'
                    order by relname
                    LIMIT 500
                    """
result_showtable = db_gp.execute(query_showtable)
rows_showtable = result_showtable.fetchall()
table_names_showtable = [row[0] for row in rows_showtable]

# 示例 SQL 查询语句列表
sql_offline = f'''SELECT job_sql FROM ods_gp_job_execute_log group by job_sql '''

# 执行 SQL 查询，获取sql结果集
result_offline = db_gp.execute(sql_offline)
job_sql_list_offline = [row[0] for row in result_offline]

all_table_pairs = []
for sql_query in job_sql_list_offline:
    source_tables, target_tables = parse_source_target_tables(sql_query)
    table_pairs = [(source_table, target_table) for source_table in source_tables for target_table in target_tables]
    all_table_pairs.extend(table_pairs)

all_table_pairs = list(set(all_table_pairs))

all_table_pairs = [pair for pair in all_table_pairs if pair[0] != pair[1]]

all_table_pairs = [(source_table, target_table) for source_table, target_table in all_table_pairs
                   if source_table in table_names_showtable and target_table in table_names_showtable]

selected_table = ['ads_rr_demand_qty']

# 从 job_sync 列表中筛选出符合指定表名的数据
table_lineage = [item for item in all_table_pairs if any(table in item for table in selected_table)]

print(table_lineage)
