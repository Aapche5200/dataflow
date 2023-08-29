from flask import Flask, request, render_template
import pandas as pd
import time
from table_parsing import parse_source_target_tables
from default_engine import get_default_engin
default_engine = get_default_engin()


def show_property():
    table_names = show_table_names()
    return render_template('property.html', table_names=table_names)


def show_table_names():
    # 默认显示30个表
    # 使用engine.execute方法执行查询并返回结果
    # 这里只是一个示例，你需要根据你的实际数据库结构和查询语句进行实现
    query = f"""
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'job_task'
    LIMIT 500
    """
    result = default_engine.execute(query)
    rows = result.fetchall()
    table_names = [row[0] for row in rows]
    return table_names


def show_table_details(table_name):
    # 根据表名从数据库获取表的详细信息的逻辑
    # 使用engine.execute方法执行查询并返回结果
    # 这里只是一个示例，你需要根据你的实际数据库结构和查询语句进行实现
    query = f'''
    SELECT
	t.TABLE_NAME AS table_name,
	t.TABLE_COMMENT AS table_comment,
	job_owner table_owner 
    FROM
	information_schema.TABLES t
	LEFT JOIN ods_task_job_schedule_pool AS p 
	ON t.table_name = substr( p.job_name, 6, length( p.job_name )- 5 ) 
    WHERE
	t.TABLE_SCHEMA = 'job_task' 
	AND t.TABLE_NAME = '{table_name}'
                    '''
    result = default_engine.execute(query)
    table_details = result.fetchone()

    query = f'''
    SELECT
	COLUMN_NAME AS column_name,
	COLUMN_COMMENT AS column_comment,
	COLUMN_TYPE AS data_type 
    FROM
	information_schema.COLUMNS 
    WHERE
	TABLE_SCHEMA = 'job_task' 
	AND TABLE_NAME = '{table_name}';
              '''
    result = default_engine.execute(query)
    table_columns = result.fetchall()

    current_develop_tables = show_table_names()

    # SQL脚本
    sql_sync = f'''select job_db, job_name,job_sql , job_level, job_owner
        from ods_task_job_schedule_pool 
        where  job_type='数据同步' 
        order by level_sort'''
    df_sync = pd.read_sql(sql_sync, default_engine)

    # 解析 数据同步涉及到的表
    job_tablelist = df_sync['job_sql']
    job_tablelist_source = job_tablelist.str.split('-->').str[0]
    job_tablelist_target = job_tablelist.str.split('-->').str[1]
    job_sync = list(zip(job_tablelist_source, job_tablelist_target))
    # 把数据同步转成list
    sync_tables = job_tablelist_source.values.tolist()
    # 数据同步的表和当前开发环境表组成血缘源数据表，让后期解析的SQL进行比对
    source_ties_tables = current_develop_tables + sync_tables
    source_ties_tables = list(set(source_ties_tables))

    # 示例 SQL 查询语句列表
    sql_offline = f'''SELECT job_sql FROM ods_task_job_execute_log group by job_sql '''

    # 执行 SQL 查询，获取离线任务sql结果集
    result_offline = default_engine.execute(sql_offline)
    job_sql_list_offline = [row[0] for row in result_offline]

    # 解析多个sql,并把相关表存储
    all_table_pairs = []
    for sql_query in job_sql_list_offline:
        # 解析出源表和目标
        source_tables, target_tables = parse_source_target_tables(sql_query)
        # 源表和目标形成映射关系
        table_relations_offline = [(source_table, target_table) for source_table in
                                   source_tables for target_table in
                                   target_tables]
        # 存储在一个变量
        all_table_pairs.extend(table_relations_offline)
    # 去重
    all_table_pairs = list(set(all_table_pairs))
    # 同步的表和解析离线任务的表合并
    combine_list = job_sync + all_table_pairs
    # 剔除 源表和目标是同一个表
    combine_list = [pair for pair in combine_list if pair[0] != pair[1]]
    # 剔除可能是子查询解析出来的表，
    # 判断这些表前提是在数据库且是被同步的源数据库表，
    # 才能展示相关血缘关系
    combine_list = [(source_table, target_table) for source_table, target_table in
                    combine_list
                    if source_table in source_ties_tables and
                    target_table in source_ties_tables]

    selected_table = [table_name]

    # 从列表中筛选出符合指定表名的数据
    table_lineage = [item for item in combine_list if
                     any(table in item for table in selected_table)]

    # 以类似于JavaScript变量的格式创建状态列表和edg列表
    state = []
    edg = []

    node_id_map = {}  # 跟踪节点ID

    for start, end in table_lineage:
        if start not in node_id_map:
            node_id_map[start] = len(node_id_map) + 1
            label = f'{start}'
            if len(label) > 15:
                lines = [label[i:i + 15] for i in range(0, len(label), 15)]
                label = '\n'.join(lines)
            state.append({
                'id': node_id_map[start],
                'label': label,
                'class': 'type-suss'  # 设置默认样式
            })

        if end not in node_id_map:
            node_id_map[end] = len(node_id_map) + 1
            label = f'{end}'
            if len(label) > 15:
                lines = [label[i:i + 15] for i in range(0, len(label), 15)]
                label = '\n'.join(lines)
            state.append({
                'id': node_id_map[end],
                'label': label,
                'class': 'type-suss'  # 设置默认样式
            })

        edg.append({
            'start': node_id_map[start],
            'end': node_id_map[end],
            'option': {}
        })

    return render_template('property.html', table_details=table_details,
                           table_columns=table_columns,
                           table_names=current_develop_tables,
                           nodes_list=state,
                           edges=edg
                           )
