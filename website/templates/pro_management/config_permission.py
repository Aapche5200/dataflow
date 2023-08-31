from bs4 import BeautifulSoup
from flask import request
from templates.offline_task.schedule_info import default_engine


def get_menus():
    # 读取HTML文件
    with open('templates/layout.html', 'r', encoding='utf-8') as file:
        html = file.read()

    # 使用Beautiful Soup解析HTML
    soup = BeautifulSoup(html, 'html.parser')

    # 获取<a>标签内的文本值
    a_tags = soup.find_all('a')
    # 存储文本值的列表
    text_values = []

    # 遍历每个<a>标签并获取文本值
    for a_tag in a_tags:
        text = a_tag.get_text()
        text_values.append(text)

    return text_values


def edit_permission():
    if request.method == "POST" and "save_button" in request.form:
        user_name = request.form.get("user_name")
        permissions = request.form.get("dependencies")
        # Split the dependencies string into a list
        permission_values = [dep.strip() for dep in permissions.split(',')]

        # Get the existing dependencies for the given task_name
        existing_permission_query = '''
                SELECT user_menu
                FROM user_permissions
                WHERE user_name = %s
            '''
        existing_permission_rows = default_engine.execute(existing_permission_query,
                                                          (user_name,)).fetchall()

        # Convert the existing dependencies to a list
        existing_permission_values = [row['user_menu'] for row in
                                      existing_permission_rows]

        # Insert a row for each combination of task_name and a dependency
        for dep_value in existing_permission_values:
            if dep_value not in permission_values:
                # Delete entries that are no longer in the new input
                delete_query = '''
                                DELETE FROM user_permissions
                                WHERE user_name = %s AND user_menu = %s
                            '''
                default_engine.execute(delete_query, (user_name, dep_value))

        for dep_value in permission_values:
            if dep_value not in existing_permission_values:
                job_name = f"{dep_value}&{user_name}"
                # Insert each combination into your database table
                insert_query = '''
                        replace INTO user_permissions 
                        (user_menu, user_name)
                        VALUES (%s, %s)
                    '''
                values = (dep_value, user_name)

                # Execute the insert query using your database engine (default_engine)
                default_engine.execute(insert_query, values)

    delete_null_query = '''
    DELETE FROM user_permissions
    WHERE user_name ='' or 
    user_menu =''
    '''
    default_engine.execute(delete_null_query)


def show_edit_permission():
    user_permission_query = f'''
            select 
            user_menu,
            user_name
            from user_permissions
        '''
    user_permission_list = \
        default_engine.execute(user_permission_query).fetchall()

    # 创建一个新的列表来存储结果
    result_permissions = []

    # 创建一个字典，用于将相同的第二个元素合并在一起
    merged_data = {}

    # 遍历原始数据
    for item in user_permission_list:
        key = item[1]  # 第二个元素作为键
        value = item[0]  # 第一个元素作为值

        # 如果字典中已经有了这个键，将值添加到已存在的值后面，用换行符分隔
        if key in merged_data:
            merged_data[key] += '<br>' + value
        else:
            merged_data[key] = value

    # 将字典转换为列表，同时将键和值拼接成元组
    for key, value in merged_data.items():
        result_permissions.append((value, key))

    return result_permissions
