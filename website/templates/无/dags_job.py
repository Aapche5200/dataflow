from flask import Flask, render_template, request, redirect
from templates.data_work.get_task_info import dags_job_all
from templates.offline_task.schedule_info import default_engine


def task_node_job():
    selected_dagsjobs = request.form.getlist('selected_dagsjobs[]')
    action = request.form.get('action')
    if len(selected_dagsjobs) > 0:
        if action == '确定删除':
            delete_jobs(selected_dagsjobs)

    if request.method == 'POST' and len(selected_dagsjobs) <= 0:
        if action == '添加':
            insert_job()

    task_node_message = task_node_list()
    dags_job_list = dags_job_all(default_engine)

    return render_template('/dags/html/dags_job.html',
                           task_node_message=task_node_message,
                           dags_job_message=dags_job_list
                           )


def task_node_list():
    task_node_query = f'''
            select 
            t1.job_name
            from ods_task_job_schedule_pool as t1
        '''
    task_node_message = default_engine.execute(task_node_query).fetchall()
    cleaned_result = []
    for row in task_node_message:
        cleaned_row = [str(item) for item in row]  # 转换为字符串列表
        cleaned_result.append(', '.join(cleaned_row))  # 使用逗号连接元素

    return cleaned_result


def insert_job():
    job_parent_node = request.form.get('source')
    job_child_node = request.form.get('destination')
    job_owner = request.form.get('job_owner')

    # 执行插入操作
    insert_query = '''
    INSERT INTO dags_jobs (
    job_parent_node,
    job_child_node,
    job_owner
            ) VALUES (%s, %s, %s)
    '''
    default_engine.execute(insert_query, (
        job_parent_node,
        job_child_node,
        job_owner))


def delete_jobs(selected_jobs):
    data_list = selected_jobs

    for data in data_list:
        # 使用逗号分割字符串并去除首尾空格
        split_values = data.split(',')
        value_parent = split_values[0].strip()
        value_child = split_values[1].strip()

        # Construct the DELETE SQL query
        sql = f'''
        DELETE 
        FROM dags_jobs 
        WHERE  
        job_parent_node ='{value_parent}' and 
        job_child_node='{value_child}'
        '''

        try:
            # Execute the DELETE query with the selected job names as parameters
            default_engine.execute(sql)
        except Exception as e:
            # Handle any errors that occur during the deletion
            print(f"Error deleting jobs: {e}")
