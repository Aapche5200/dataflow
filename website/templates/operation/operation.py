from flask import session, render_template, request
import datetime
from templates.data_work.get_task_info import get_task_result
from templates.offline_task.schedule_info import default_engine


def operation():
    now_date_center = request.form.get('now_date')
    # 存储时间参数在会话中
    session['now_date'] = now_date_center
    if not now_date_center:
        now_date_center = datetime.date.today().strftime("%Y-%m-%d")
    operation_message = get_task_result(default_engine, now_date_center)
    return render_template('/operation/html/operation.html',
                           operation_message=operation_message, now_date=now_date_center)
