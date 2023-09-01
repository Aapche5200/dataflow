from ah.website.templates.offline_task.schedule_info import default_engine
from ah.website.templates.data_work.get_task_info import get_users
from flask import Flask, request, render_template, session

app = Flask(__name__)


@app.route('/layout', methods=['GET', 'POST'])
def pages_permissions():
    username = get_users()
    user_permission_query = f'''
            select 
            user_menu
            from user_permissions
            where user_name='{username}'
        '''
    user_permission_list = \
        default_engine.execute(user_permission_query).fetchall()
    print(username)

    return render_template('layout.html',
                           user_permission_list=user_permission_list
                           )


if __name__ == '__main__':
    app.run()