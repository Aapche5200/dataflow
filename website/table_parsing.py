import re


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
    with_tables = [tbl.strip() for tbl in
                   re.findall(r'(\w+)\s+AS', ','.join(with_tables))]
    source_tables = {table for table in source_tables if
                     not any(table.startswith(tbl) for tbl in with_tables)}
    target_tables = {table for table in target_tables if
                     not any(table.startswith(tbl) for tbl in with_tables)}

    # 剔除筛选出以“temp”开头的表
    source_tables = {table for table in source_tables if not table.startswith('temp')}
    target_tables = {table for table in target_tables if not table.startswith('temp')}

    return source_tables, target_tables
