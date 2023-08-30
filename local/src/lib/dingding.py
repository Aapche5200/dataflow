# -*- coding: utf-8 -*-
import requests
import json


def post_dingding_text(message, type='notice'):
    # 专门用来给自己传递消息,传入消息字符串
    message = '通知:' + message[:5000]
    postDict = {'msgtype': "text", 'text': {'content': message}}
    headers = {"Content-Type": "application/json ;charset=utf-8 "}
    # 该url是钉钉机器人申请的url
    if type == 'silence':
        access_token = '01462c91ce9531d7f0962695b5164744d684a8938ffca7306ee280f5332f75a5'
    else:
        access_token = '12969b88bcfd45f870e833f85fe3def4f1ab278906bcc617981ce6788f9e2b27'
    url = f'''https://oapi.dingtalk.com/robot/send?access_token={access_token}'''
    resoponse = requests.post(url, data=json.dumps(postDict), headers=headers)

    return resoponse.text
