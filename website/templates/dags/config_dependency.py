from templates.data_work.get_task_info import dags_job_all
from flask import request


def task_node_list(default_engine):
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


def edit_node(default_engine):
    if request.method == "POST" and "save_button" in request.form:
        task_name = request.form.get("task_name")
        dependencies = request.form.get("dependencies")
        print(task_name, dependencies)
        # Split the dependencies string into a list
        dependency_values = [dep.strip() for dep in dependencies.split(',')]

        # Get the existing dependencies for the given task_name
        existing_dependencies_query = '''
                SELECT job_parent_node
                FROM dags_jobs
                WHERE job_child_node = %s
            '''
        existing_dependency_rows = default_engine.execute(existing_dependencies_query,
                                                          (task_name,)).fetchall()

        # Convert the existing dependencies to a list
        existing_dependency_values = [row['job_parent_node'] for row in
                                      existing_dependency_rows]

        # Insert a row for each combination of task_name and a dependency
        for dep_value in existing_dependency_values:
            if dep_value not in dependency_values:
                # Delete entries that are no longer in the new input
                delete_query = '''
                                DELETE FROM dags_jobs
                                WHERE job_child_node = %s AND job_parent_node = %s
                            '''
                default_engine.execute(delete_query, (task_name, dep_value))

        for dep_value in dependency_values:
            if dep_value not in existing_dependency_values:
                job_name = f"{dep_value}&{task_name}"
                # Insert each combination into your database table
                insert_query = '''
                        replace INTO dags_jobs 
                        (job_name,job_parent_node, job_child_node)
                        VALUES (%s, %s, %s)
                    '''
                values = (job_name, dep_value, task_name)

                # Execute the insert query using your database engine (default_engine)
                default_engine.execute(insert_query, values)

    delete_null_query = '''
    DELETE FROM dags_jobs
    WHERE job_child_node ='' or 
    job_parent_node =''
    '''
    default_engine.execute(delete_null_query)


def show_edit_nodes(default_engine):
    dags_nodes = dags_job_all(default_engine)

    # 创建一个新的列表来存储结果
    result_nodes = []

    # 创建一个字典，用于将相同的第二个元素合并在一起
    merged_data = {}

    # 遍历原始数据
    for item in dags_nodes:
        key = item[1]  # 第二个元素作为键
        value = item[0]  # 第一个元素作为值

        # 如果字典中已经有了这个键，将值添加到已存在的值后面，用换行符分隔
        if key in merged_data:
            merged_data[key] += '<br>' + value
        else:
            merged_data[key] = value

    # 将字典转换为列表，同时将键和值拼接成元组
    for key, value in merged_data.items():
        result_nodes.append((value, key))

    return result_nodes
