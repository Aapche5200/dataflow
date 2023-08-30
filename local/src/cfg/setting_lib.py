# -*- coding: utf-8 -*-
from pathlib import Path

log_dict = {'kw': 'datatrans',  # 每个日志系统文件的前缀
            'path': Path(__file__).parent.parent.parent.joinpath('logs'),  # 日志的路径
            'sh': 'error',  # 控制台是否输出，输出的日志等级
            'levels': ['error', 'info'],  # 日志等级文件
            }

excel_dict = {'path': Path(__file__).parent.parent.parent.joinpath('excel')  # 默认excel路径
              }
