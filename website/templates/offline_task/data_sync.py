# -*- coding: utf-8 -*-
# coding=utf8
from flask import Flask, render_template, request, redirect
import time
import random
import logging
import concurrent.futures
from sqlalchemy.sql import text
import traceback
from db_con import DbCon
from templates.dags.dags_task import execute_dependency_jobs
import pandas as pd
from templates.pro_management.add_users import get_registered_users
from templates.offline_task.schedule_info import task_scheduler, default_engine
from templates.dags.config_dependency import task_node_list, edit_node, show_edit_nodes
from templates.data_work.get_task_info import get_users


def sync_tasks():
    selected_jobs = request.form.getlist('selected_jobs[]')
    action = request.form.get('action')
    if len(selected_jobs) > 0:
        tasks = get_tasks_from_db(selected_jobs)
        if action == '确定删除':
            # Delete selected jobs from the database
            delete_jobs(selected_jobs)
        elif action == '停用':
            # Update the status of selected jobs to '停用'
            update_job_status(selected_jobs, '停用')
        elif action == '启用':
            # Update the status of selected jobs to '启用'
            update_job_status(selected_jobs, '启用')
        elif action == '立即执行':
            for task_excute in tasks:
                job_name = task_excute[0]
                execute_sql(job_name)
        elif action == '提交任务':
            if len(tasks) > 0:
                # Schedule tasks
                for task in tasks:
                    job_name = task[0]
                    task_frequency = task[1]
                    task_time = task[2]
                    task_status = task[3]
                    if not task_scheduler.get_job(job_id=job_name):
                        if task_status == '启用':
                            hour = int(task_time.split(':')[0])
                            minute = int(task_time.split(':')[1])
                            if task_frequency == '按天':
                                task_scheduler.add_job(execute_sql, 'cron',
                                                       hour=str(hour),
                                                       minute=str(minute), second='0',
                                                       args=[str(job_name)],
                                                       jitter=random.randint(0, 60),
                                                       misfire_grace_time=600,
                                                       replace_existing=True
                                                       , id=str(job_name))
                            elif task_frequency == '实时':
                                task_scheduler.add_job(execute_sql, 'interval',
                                                       hours=hour,
                                                       minutes=minute, seconds=0,
                                                       args=[str(job_name)],
                                                       jitter=random.randint(0, 60),
                                                       misfire_grace_time=600,
                                                       replace_existing=True,
                                                       id=str(job_name))
    # 判断所有任务的状态是否为停用，如果是则暂停并移除调度任务
    tasks_all = get_tasks_all()
    for remove_task in tasks_all:
        remove_job_name = remove_task[0]
        remove_task_status = remove_task[3]
        if task_scheduler.get_job(
                job_id=str(remove_job_name)) and remove_task_status == '停用':
            if task_scheduler.state == 1:
                task_scheduler.pause_job(job_id=str(remove_job_name))
                task_scheduler.remove_job(job_id=str(remove_job_name))
    message = '---任务名必须唯一---'
    if request.method == 'POST' and len(selected_jobs) <= 0:
        if action == '添加':
            existing_tasks = get_tasks_all()
            job_name = request.form.get('job_name')
            # Check if the new job_name already exists
            if any(task['job_name'] == job_name for task in existing_tasks):
                message = '名称已存在，请重新输入！！！'
            else:
                insert_job()
        elif action == '更新':
            update_job()

    execute_dependency_jobs(default_engine)
    execute_message = get_tasks_all()
    user_list_quote = get_registered_users()
    node_lists = task_node_list(default_engine)
    edit_node(default_engine)
    result_nodes = show_edit_nodes(default_engine)

    return render_template('/offline_task/html/data_sync.html',
                           user_list_quote=user_list_quote,
                           message=message,
                           node_lists=node_lists,
                           result_nodes=result_nodes,
                           execute_message=execute_message)


def get_tasks_all():
    username = get_users()
    execute_result_query = f'''
            select job_name,job_db,job_sql,job_status,
            job_owner,job_desc,
            job_frequency,job_time 
            from ods_task_job_schedule_pool
            where job_type='数据同步' and 
            if('{username}' like 'admin%%',1=1,job_owner='{username}')
        '''
    tasks_all = default_engine.execute(execute_result_query).fetchall()

    return tasks_all


def get_tasks_from_db(selected_jobs):
    task_sql = "SELECT job_name, job_frequency, job_time,job_status " \
               "FROM ods_task_job_schedule_pool " \
               "WHERE job_type='数据同步' and  job_name IN ({})".format(
        ','.join(['%s'] * len(selected_jobs)))
    tasks = default_engine.execute(task_sql, tuple(selected_jobs)).fetchall()

    return tasks


def insert_job():
    job_name = request.form.get('job_name')
    job_desc = request.form.get('job_desc')
    job_owner = request.form.get('job_owner')
    job_frequency = request.form.get('task_frequency')
    job_time = request.form.get('task_time')
    db_type = request.form.get('databaseSelect1')
    db_tablelist = request.form.get('db_tablelist')
    target_db_type = request.form.get('databaseSelect2')
    target_db_tablelist = request.form.get('target_db_tablelist')
    db_type_combined = db_type + '-->' + target_db_type
    db_tablelist_combined = db_tablelist + '-->' + target_db_tablelist

    # 执行插入操作
    insert_query = '''
    INSERT INTO ods_task_job_schedule_pool (job_level, job_type,level_sort,
            job_name,
            job_db,
            job_sql,
            job_desc,
            job_owner,
            job_frequency,
            job_time
            ) VALUES ('ods','数据同步',111,%s, %s, %s, %s, %s, %s, %s)
    '''
    default_engine.execute(insert_query, (job_name,
                                          db_type_combined,
                                          db_tablelist_combined,
                                          job_desc,
                                          job_owner,
                                          job_frequency,
                                          job_time))


def update_job():
    job_name = request.form.get('job_name')
    job_desc = request.form.get('job_desc')
    job_owner = request.form.get('job_owner')
    job_frequency = request.form.get('task_frequency')
    job_time = request.form.get('task_time')
    db_type = request.form.get('databaseSelect1')
    db_tablelist = request.form.get('db_tablelist')
    target_db_type = request.form.get('databaseSelect2')
    target_db_tablelist = request.form.get('target_db_tablelist')
    db_type_combined = db_type + '-->' + target_db_type
    db_tablelist_combined = db_tablelist + '-->' + target_db_tablelist

    # 查询现有记录
    select_query = '''
    SELECT * FROM ods_task_job_schedule_pool WHERE job_name = %s
    '''
    result = default_engine.execute(select_query, (job_name,))
    existing_job = result.fetchone()

    # 判断要更新的字段是否输入了值，并执行更新操作
    update_query = '''
    UPDATE ods_task_job_schedule_pool SET job_db = %s,
    job_sql = %s, job_desc = %s, job_owner = %s, job_frequency = %s, job_time = %s
    WHERE  job_type='数据同步' and job_name = %s
    '''
    update_data = []

    if db_type and target_db_type:
        update_data.append(db_type_combined)
    elif db_type or target_db_type:
        if db_type:
            update_data.append(db_type + '-->' + existing_job['job_db'].split('-->')[-1])
        else:
            update_data.append(
                existing_job['job_db'].split('-->')[0] + '-->' + target_db_type)
    else:
        update_data.append(existing_job['job_db'])

    if db_tablelist and target_db_tablelist:
        update_data.append(db_tablelist_combined)
    elif db_tablelist or target_db_tablelist:
        if db_tablelist:
            update_data.append(
                db_tablelist + '-->' + existing_job['job_sql'].split('-->')[-1])
        else:
            update_data.append(
                existing_job['job_sql'].split('-->')[0] + '-->' + target_db_tablelist)
    else:
        update_data.append(existing_job['job_sql'])

    if job_desc:
        update_data.append(job_desc)
    else:
        update_data.append(existing_job['job_desc'])

    if job_owner:
        update_data.append(job_owner)
    else:
        update_data.append(existing_job['job_owner'])

    if job_frequency:
        update_data.append(job_frequency)
    else:
        update_data.append(existing_job['job_frequency'])

    if job_time:
        update_data.append(job_time)
    else:
        update_data.append(existing_job['job_time'])

    update_data.append(job_name)

    default_engine.execute(update_query, tuple(update_data))


def delete_jobs(selected_jobs):
    # Construct the DELETE SQL query
    sql = f"DELETE FROM ods_task_job_schedule_pool " \
          f"WHERE  job_type='数据同步' and job_name IN ({','.join(['%s'] * len(selected_jobs))})"

    try:
        # Execute the DELETE query with the selected job names as parameters
        default_engine.execute(sql, tuple(selected_jobs))
    except Exception as e:
        # Handle any errors that occur during the deletion
        print(f"Error deleting jobs: {e}")


def update_job_status(selected_jobs, status):
    # Construct the UPDATE SQL query
    sql = f"UPDATE ods_task_job_schedule_pool " \
          f"SET job_status = %s " \
          f"WHERE  job_type='数据同步' and job_name IN ({','.join(['%s'] * len(selected_jobs))})"

    try:
        # Execute the UPDATE query with the selected job names and status as parameters
        default_engine.execute(sql, (status,) + tuple(selected_jobs))
    except Exception as e:
        # Handle any errors that occur during the update
        print(f"Error updating job status: {e}")


# 创建映射类型映射
def map_data_type(sql_server_type):
    if sql_server_type.startswith('nvarchar'):
        return 'varchar' + sql_server_type[8:]
    elif sql_server_type == 'int':
        return 'integer'
    elif sql_server_type == 'datetime':
        return 'timestamp'
    # 继续根据需要添加其他数据类型映射
    else:
        return sql_server_type


# 执行SQL脚本的函数
def execute_sql(job_name_exe):
    # SQL脚本
    sql = f'''select job_db, job_name,job_sql , job_level, job_owner
        from ods_task_job_schedule_pool 
        where job_status='启用' and  job_type='数据同步' and
        job_name ='{job_name_exe}'
        order by level_sort'''
    df = pd.read_sql(sql, default_engine)
    deal_infos = []
    for index, (job_db, job_name, job_sql, job_level, job_owner) in df.iterrows():
        begin_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        try:
            job_dbsource = job_db.split('-->')[0]
            job_dbtarget = job_db.split('-->')[1]
            job_tablelist_source = job_sql.split('-->')[0]
            job_tablelist_target = job_sql.split('-->')[1]

            if job_dbsource in DbCon().engines and job_dbtarget in DbCon().engines:
                source_con = DbCon().engines[job_dbsource]
                target_con = DbCon().engines[job_dbtarget]
                # 2. 获取源表列信息
                columns = source_con.execute(
                    f'''
                    SELECT 
                    column_name, 
                    data_type FROM 
                    INFORMATION_SCHEMA.COLUMNS 
                    WHERE table_name = '{job_tablelist_source}'
                    ''').fetchall()

                # 3. 生成创建表的SQL语句
                create_table_sql = f"CREATE TABLE IF NOT EXISTS {job_tablelist_target} ("
                for column in columns:
                    column_name = column[0]
                    create_table_sql += f"{column_name} TEXT,"  # 将所有字段设置为文本类型
                create_table_sql = create_table_sql.rstrip(",") + ");"

                if not target_con.execute(
                        f'''
                        SELECT table_name 
                        FROM information_schema.tables 
                        WHERE table_name = '{job_tablelist_target}'
                        ''').fetchone():
                    target_con.execute(create_table_sql)

                # 清空目标表数据
                target_con.execute(text(f"TRUNCATE TABLE {job_tablelist_target}"))

                # 4. 获取源表数据并批量插入目标表
                batch_size = 500
                offset = 0

                # 执行数据插入
                target_columns = [column[0] for column in target_con.execute(
                    f'''
                    SELECT column_name 
                    FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE table_name = '{job_tablelist_target}'
                    '''
                ).fetchall()]

                first_column = target_columns[0]

                # 定义一个函数来处理数据块并插入目标表
                def process_data_and_insert(datax):
                    # 将所有列转换为文本类型
                    data = datax.fillna('')
                    data = data.astype('str')
                    # data = data.replace('NaT', '')

                    insert_query = (
                        f'''
                        INSERT INTO 
                        {job_tablelist_target}  
                        ({', '.join(target_columns)}) VALUES 
                        ({','.join([':' + column_t for column_t in target_columns])})
                        ''')

                    values = [{column: row[column] for column in target_columns} for
                              _, row in
                              data.iterrows()]
                    target_con.execute(text(insert_query), values)

                while True:
                    try:
                        query = f'''
                                           SELECT * 
                                           FROM {job_tablelist_source}
                                           order by {first_column}
                                           OFFSET {offset} ROWS FETCH FIRST {batch_size} ROWS ONLY
                                           '''
                        data = pd.read_sql(query, source_con)
                    except:
                        query = f'''
                                           SELECT * 
                                           FROM {job_tablelist_source}
                                           order by {first_column}
                                           limit {batch_size}
                                           OFFSET {offset}
                                           '''
                        data = pd.read_sql(query, source_con)

                    # 对特定列进行编码解码操作
                    if '身份' in data.columns:
                        data['身份'] = data['身份'].apply(
                            lambda x: x.encode('latin1').decode('gbk'))

                    # 如果没有数据了，退出循环
                    if data.empty:
                        break

                    # 使用多线程并发处理数据块
                    with concurrent.futures.ThreadPoolExecutor(
                            max_workers=100) as executor:
                        executor.map(process_data_and_insert, [data])

                    offset += batch_size
                    print(offset)

                job_result = 'T'
            else:
                job_result = '未找到对应的数据源或目标'
            print(job_result)
        except Exception as e:
            job_result = 'F'
            logging.basicConfig(filename='log_record.txt',
                                level=logging.DEBUG, filemode='w',
                                format='[%(asctime)s] [%(levelname)s] >>>  %(message)s',
                                datefmt='%Y-%m-%d %I:%M:%S')
            logging.error("Main program error:")
            logging.error(e)
            logging.error(traceback.format_exc())
            print(job_result)
        end_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        deal_infos.append(
            [job_name, job_sql, begin_time, end_time, job_result, job_db, job_level,
             job_owner])
    columns = ['job_name', 'job_sql', 'begin_time', 'end_time', 'job_result', 'job_db',
               'job_level', 'job_owner']
    job_deal_df = pd.DataFrame(deal_infos, columns=columns)
    table = 'ods_task_job_execute_log'
    job_deal_df.to_sql(table, default_engine, if_exists='append', index=False)
    execute_dependency_jobs(default_engine)
