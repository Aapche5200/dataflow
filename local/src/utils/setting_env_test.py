import time

hour_minute = time.strftime('%H:%M', time.localtime(time.time() + 60))

type = 3

# 环境切换
type_dict = {0: [0, 0], 1: [0, 1], 2: [1, 0], 3: [1, 1]}

# 0是测试环境，1是正式
sql_con = {0: 'bXNzcWwrcHltc3NxbDovL3NqZng6S0dWZk1ObnJiZGRQUHVTTkAxMC4yMC4xMjEuMjI2L0FCVjU=',
           1: 'bXNzcWwrcHltc3NxbDovL3NqZng6NVZ2WFRXUyVuZmJQUmV4MVNiQDEwLjIwLjEyMC4xMTQvQUJWNQ=='}
pg_con = {1: 'cG9zdGdyZXNxbCtwc3ljb3BnMjovL3BnYWRtaW46RUYyQlI0S3lrSG81N0ZAMTAuMjAuMTIwLjE4MjozMjAwMC9yZXBvcnQ=',
          0: 'cG9zdGdyZXNxbCtwc3ljb3BnMjovL3BnYWRtaW46RUYyQlI0S3lrSG81N0ZAMTAuMjAuMTIwLjE4MjozMjAwMC90ZXN0'}
pgbk = {1: 'cG9zdGdyZXNxbCtwc3ljb3BnMjovL3BnYWRtaW46RUYyQlI0S3lrSG81N0ZAMTAuMjAuMTIwLjE4MjozMjAwMC9kYXRhX2Jr',
        0: 'cG9zdGdyZXNxbCtwc3ljb3BnMjovL3BnYWRtaW46RUYyQlI0S3lrSG81N0ZAMTAuMjAuMTIwLjE4MjozMjAwMC90ZXN0'}
ck_db = {0: 'dGVzdA==1', 1: 'b2FfZGF0YQ=='}
db_state = {'ck': 0, 'oa': 0}
db_state = {'oa': type_dict.get(type)[0], 'pg': type_dict.get(type)[1], 'ck': type_dict.get(type)[1], }
# db_state = {'ck': 0, 'oa': 0}
# print(db_state)

env_dict = {
    'env_info': {
        'ss': sql_con.get(type_dict.get(type)[0]),
        'zentao': 'bXlzcWwrcHlteXNxbDovL2RhdGFxdWVyeTpAZXZKaUFnQTRJTzREZXpUQDEwLjIwLjEyMS4yNToxMjMyMy96ZW50YW9lcA==',
        'ck_host': 'MTAuMjAuMTIwLjI4',
        'ck_port': 'OTEwMQ==',
        'ck_db': ck_db.get(type_dict.get(type)[1]),
        'pg': pg_con.get(type_dict.get(type)[1]),
        'pgbk': pgbk.get(type_dict.get(type)[1]),
        'ck_user': 'Y2hyaXNfeWU=',
        'ck_password': 'TnYyS2h3c2E=',
    },
    'db_state': db_state,
    'debug': True,
    'schedule_job_dict': { \
        # 'log_parse_alarm': {'schedule_time': hour_minute},
        'exit': {'schedule_time': '04:45'},

    },

}
