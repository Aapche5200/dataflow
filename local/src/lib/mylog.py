# -*- coding: utf-8 -*-
import logging
import time
from pathlib import Path

log_dict = {'kw': 'kw',  # 每个日志系统文件的前缀
            'path': Path(__file__).parent.parent.parent.joinpath('logs'),  # 日志的路径
            'sh': 'error',  # 控制台是否输出，输出的日志等级
            'levels': ['error', 'info'],  # 日志等级文件
            }
cfg_file = Path(__file__).parent.parent / 'cfg' / 'setting_lib.py'
if cfg_file.exists():
    from cfg.setting_lib import log_dict


class MyLog:
    def __init__(self):
        # 创建自己的日志收集器
        self.log = logging.getLogger("my_log")
        # 设置收集的日志等级，设置为DEBUG等级
        self.log.setLevel("DEBUG")

        # 设置日志输出的格式,可以通过logging.Formatter指定日志的输出格式
        ft = "%(asctime)s |%(levelname)s|%(module)s:%(lineno)d| %(message)s"  # 工作中常用的日志格式
        ft = logging.Formatter(ft)

        logpath = log_dict.get('path')
        if not logpath.exists():
            logpath.mkdir(parents=True, exist_ok=True)
        month_str = time.strftime('%Y%m', time.localtime(time.time()))
        file_prefix = log_dict.get('kw') + month_str

        # 创建一个日志输出渠道（输出到控制台），并且设置输出的日志等级
        sh_level = log_dict.get('sh')
        if sh_level:
            l_s = logging.StreamHandler()
            l_s.setLevel(sh_level.upper())
            # 将日志输出渠道添加到日志收集器中
            self.log.addHandler(l_s)
            # 设置控制台和日志文件输出日志的格式
            l_s.setFormatter(ft)

        # 设置输出的日志等级为ERROR以上
        if 'error' in log_dict.get('levels'):
            l_f = logging.FileHandler(logpath / f"{file_prefix}_error.log", encoding='utf8')
            l_f.setLevel("ERROR")
            self.log.addHandler(l_f)
            l_f.setFormatter(ft)

        # # 设置输出的日志等级为DEBUG以上
        if 'debug' in log_dict.get('levels'):
            l_d = logging.FileHandler(logpath / f"{file_prefix}_debug.log", encoding='utf-8')
            l_d.setLevel("DEBUG")
            self.log.addHandler(l_d)
            l_d.setFormatter(ft)

        # 设置输出的日志等级为INFO以上
        if 'info' in log_dict.get('levels'):
            l_i = logging.FileHandler(logpath / f"{file_prefix}_info.log", encoding='utf-8')
            l_i.setLevel("INFO")
            self.log.addHandler(l_i)
            l_i.setFormatter(ft)


log = MyLog().log
