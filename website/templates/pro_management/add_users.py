from flask import Flask, render_template, request, redirect
import json
from templates.pro_management.config_permission import get_menus
from templates.pro_management.config_permission import edit_permission
from templates.pro_management.config_permission import show_edit_permission


def add_users():
    message = ''
    selected_jobs = request.form.getlist('selected_user[]')
    action = request.form.get('action')
    if len(selected_jobs) > 0:
        if action == '确定删除':
            delete_jobs(selected_jobs)
            message = '<删除成功，谢谢!>'
    if request.method == 'POST' and len(selected_jobs) <= 0:
        password = request.form.get('password')
        affirm_password = request.form.get('affirm_password')
        username = request.form.get('username')
        current_user = get_registered_users()
        if password == affirm_password:
            if action == '添加' and username not in current_user:
                insert_job()
                message = '<添加成功，谢谢!>'
            elif action == '更新':
                update_job()
                message = '<更新成功，谢谢!>'
            elif username in current_user:
                message = '<用户已经存在!>'
        else:
            message = '<密码不一致>'
    user_list = get_registered_users()
    menu_lists = get_menus()
    edit_permission()
    result_permissions = show_edit_permission()
    return render_template('/pro_management/html/add_users.html',
                           user_list=user_list,
                           menu_lists=menu_lists,
                           result_permissions=result_permissions,
                           message=message)


def get_registered_users():
    with open('static/config.json', 'r') as file:
        data = json.load(file)
        user_list = list(data['registered_users'].keys())
    return user_list


def insert_job():
    username = request.form.get('username')
    password = request.form.get('password')
    affirm_password = request.form.get('affirm_password')

    if password == affirm_password:
        # 读取 config.json 文件中的数据
        with open('static/config.json', 'r') as file:
            config_data = json.load(file)

        # 添加新的用户数据
        config_data['registered_users'][username] = password

        # 写入更新后的数据到 config.json 文件
        with open('static/config.json', 'w') as file:
            json.dump(config_data, file, indent=4)


def update_job():
    username = request.form.get('username')
    password = request.form.get('password')
    affirm_password = request.form.get('affirm_password')

    if password == affirm_password:
        # 读取 config.json 文件中的数据
        with open('static/config.json', 'r') as file:
            config_data = json.load(file)

        # 更新现有用户的数据
        config_data['registered_users'][username] = password

        # 写入更新后的数据到 config.json 文件
        with open('static/config.json', 'w') as file:
            json.dump(config_data, file, indent=4)


def delete_jobs(selected_user):
    # 读取配置文件
    with open('static/config.json', 'r') as file:
        data = json.load(file)

    # 删除指定用户
    user_to_delete = selected_user  # 要删除的用户名
    for user in user_to_delete:
        if user in data['registered_users']:
            del data['registered_users'][user]

    # 将修改后的数据写回到文件
    with open('static/config.json', 'w') as file:
        json.dump(data, file, indent=4)
