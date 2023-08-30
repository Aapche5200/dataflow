# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import time
from pprint import pprint
from pathlib import Path
from lib.mylog import log
from sqlalchemy.types import Date, DateTime

excel_dict = {'path': Path(__file__).parent.parent.parent.joinpath('excel')  # 默认excel路径
              }
cfg_file = Path(__file__).parent.parent / 'cfg' / 'setting_lib.py'
if cfg_file.exists():
    from cfg.setting_lib import excel_dict


def unix_to_time(unixtime):
    # 时间戳转时间
    return pd.to_datetime(unixtime, unit='s', utc=True).tz_convert('Asia/Shanghai')


def time_to_unix(dt64):
    # 时间转时间戳
    return dt64.astype('M8[S]').astype('int')


class MyDataFrame(pd.DataFrame):
    name = 'temp'

    def transType(self, type_dict):
        # 转换小数、整数、字符串、日期、时间
        # 必须采用{'int64':['col1','col2']} 这种结构。
        if not isinstance(type_dict, dict):
            raise SystemExit('MyDataFrame trans_type error')
        for k, v in type_dict.items():
            # type_dict 的keys 必须在int64，float64，string里面，限定转换内容
            if k not in ('int64', 'float64', 'string'):
                raise SystemExit('MyDataFrame trans_type error')

            if not isinstance(v, list):
                raise SystemExit('MyDataFrame trans_type error')

            for col in v:
                self[col] = self[col].astype(k)

    def transTime(self, col, before, after, new_col=None):
        # before , after 待转时间格式，其中M8代表datetime64，int代表时间戳，时间字符串格式为''
        # print(self[col].head())
        # new_col = new_col if new_col else col
        # new = pd.to_datetime(self[col])
        # print(new.head())
        # exit()

        if before == 'int':
            print(self[new_col].head())
        elif before == 'M8' and after.startswith('M8'):
            self[new_col] = self[col].astype(after)

        pass

    def transDuration(self, col, num=1, type='D', new_col=None):
        '''
        :param col:需要处理的列
        :param num: type的数量，比如type是D，代表除以几天
        :param type: {Y:year，M:month,W:week,D:day,h:hour,m:minute,s second},还有毫秒、纳秒的参数可选，一般不用那么精确
        :param new_col: 生成新的列，如果没有，则改变原来的列
        :return:
        '''
        new_col = new_col if new_col else col
        self[new_col] = self[col] / np.timedelta64(num, type)

    def showData(self):
        pprint(self)

    def showDtypes(self):
        pprint(self.dtypes)

    def fillnaALL(self):
        '''
        根据数据类型自动填充数据，字符串填充‘’，整数填充0，小数填充0.0，慎用
        '''
        # 填充数据
        field_dict = self.dtypes.astype(str).to_dict()
        str_fields = [k for k, v in field_dict.items() if v in ['object', 'string']]
        int_fields = [k for k, v in field_dict.items() if v in ['int64', 'float64', 'int32', 'float32']]
        float_fields = [k for k, v in field_dict.items() if v in ['float64', 'float32']]
        if int_fields:
            self[int_fields] = self[int_fields].fillna(0).astype('int64')
        if float_fields:
            self[float_fields] = self[float_fields].fillna(0.0).astype('float64')
        if str_fields:
            self[str_fields] = self[str_fields].fillna('').astype('string')
        [print(f'{col} columns has not fillna') for col in self.columns if
         col not in set(str_fields + int_fields + float_fields)]

    def toExcel(self, path=None, index=False, **kwargs):
        '''
        可以自动保存为excel，文件名称就是self.name
        :param path: 设定路径
        :param index: 索引一般默认不存储
        :param kwargs: 可以填充其他参数，放在pd.to_excel里面
        :return:
        '''
        # 保存excel
        if not path:
            path = excel_dict.get('path')
            if not path.exists():
                path.mkdir(parents=True, exist_ok=True)
        try:
            writer = pd.ExcelWriter(path / f'{self.name}.xlsx')
        except PermissionError:
            time_str = time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()))
            writer = pd.ExcelWriter(path / f'{self.name}_{time_str}.xlsx')
        self.to_excel(writer, index=index, **kwargs)
        writer.save()

    def toSql(self, db_code='dmpg_bk', transfer_time=None, if_exists='replace', dtype=None, index=False, **kwargs):
        # 保存数据表
        from utils.db import dbcons
        dbcon = dbcons.get(db_code)
        if self.empty:
            log.warn(f'{self.name} is empty')

        # 追加写入时间
        if not transfer_time:
            transfer_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        transfer_date = transfer_time[:10]
        df = pd.DataFrame(self)
        df['transfer_time'] = transfer_time
        df['transfer_date'] = transfer_date

        # 表格数据类型调整
        add_dtype = {'transfer_time': DateTime(), 'transfer_date': Date()}
        if not dtype:
            dtype = add_dtype
        else:
            dtype.update(add_dtype)

        df.to_sql(self.name, dbcon.connect, if_exists=if_exists, dtype=dtype, index=index, **kwargs)
