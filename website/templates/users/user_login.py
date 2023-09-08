from flask import Flask, render_template, request, redirect, session, url_for
import json


def get_user_json():
    users_json = {}
    # 加载配置文件
    with open('static/config.json', 'r') as f:
        config = json.load(f)

    # 获取 registered_users 配置
    users_json = config.get('registered_users', {})

    return users_json


def check_login():
    # 排除不需要登录检查的路由
    if request.endpoint in ['users_login','static']:
        return

    if 'username' not in session:
        return redirect(url_for('users_login'))


def login():
    registered_users = get_user_json()
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']

        if username in registered_users and registered_users[username] == password:
            session['username'] = username
            return redirect(url_for('index'))
        else:
            return render_template('/users/html/user_login.html',
                                   message='无效的用户信息,请重试')

    return render_template('/users/html/user_login.html')


def logout():
    session.pop('username', None)
    return redirect(url_for('users_login'))
