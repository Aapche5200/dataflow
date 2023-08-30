import time

hour = int(time.strftime('%H'))

hb_times = ['12:18']
if hour != 4:
    hb_times.append(time.strftime('%H:%M', time.localtime(time.time() + 60)))

env_dict = {
    'env_info': {
        'ss': 'bXNzcWwrcHltc3NxbDovL3NqZng6NVZ2WFRXUyVuZmJQUmV4MVNiQDEwLjIwLjEyMC4xMTQvQUJWNQ==',
        'zentao': 'bXlzcWwrcHlteXNxbDovL2RhdGFxdWVyeTpAZXZKaUFnQTRJTzREZXpUQDEwLjIwLjEyMS4yNToxMjMyMy96ZW50YW9lcA==',
        'pg': 'cG9zdGdyZXNxbCtwc3ljb3BnMjovL3BnYWRtaW46RUYyQlI0S3lrSG81N0ZAMTAuMjAuMTIwLjE4MjozMjAwMC9yZXBvcnQ=',
        'pgbk': 'cG9zdGdyZXNxbCtwc3ljb3BnMjovL3BnYWRtaW46RUYyQlI0S3lrSG81N0ZAMTAuMjAuMTIwLjE4MjozMjAwMC9kYXRhX2Jr',
        'ck_host': 'MTAuMjAuMTIwLjI4',
        'ck_port': 'OTEwMQ==',
        'ck_db': 'b2FfZGF0YQ==',
        'ck_user': 'Y2hyaXNfeWU=',
        'ck_password': 'TnYyS2h3c2E=',
    },
    'db_state': {},
    'logpath': '../logs',
    'debug': False,
    'schedule_job_dict': { \
        # 每日定时任务
        'exit': {'schedule_time': '04:45'},  # 重启
        'temporary_middle_table': {'schedule_time': '04:46'},  # 写中间表
        'ss2ck_transfer_job': {'schedule_time': '05:00'},  # 备份数据
        'ss2pg_transfer_job': {'schedule_time': '05:01'},
        'heart_beat': {'schedule_time': '12:18'},  # 心跳信号

        # 上午异常检查
        'table_trans_error': {'schedule_time': '08:50'},
        'clickhouse_mutations': {'schedule_time': '08:51'},
        'log_parse_alarm': {'schedule_time': '08:52'},

        # 小时准点任务
        'hour_jobs': {'schedule_type': 'hourly', 'schedule_time': ':00'},

        # 不定时任务
        'write_hr_recruit_data': {'schedule_time': ['15:00', '16:00', '17:00']},  # HR招聘数据，要求白天就可以有结果
    },
}
