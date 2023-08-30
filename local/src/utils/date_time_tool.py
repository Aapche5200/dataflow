import pandas as pd
from chinese_calendar import is_workday
import datetime
import time


def get_work_day_base():
    df = pd.DataFrame(pd.date_range('2010-01-01', '2021-12-31'), columns=['log_date'])
    df['workday'] = df['log_date'].apply(lambda x: 1 if is_workday(x) else 0)
    df['log_date'] = df['log_date'].astype(str)
    return df


work_day_base = get_work_day_base()


def get_workday_count(begin_day, end_day, contain=True):
    if contain:
        df = work_day_base.query('log_date>=@begin_day and log_date<=@end_day')
    else:
        df = work_day_base.query('log_date>@begin_day and log_date<@end_day')
    return df['workday'].sum()


def get_diff_count(begin_day, end_day):
    day_count = datetime.datetime.strptime(end_day, "%Y-%m-%d") - datetime.datetime.strptime(begin_day, "%Y-%m-%d")
    return day_count.days
