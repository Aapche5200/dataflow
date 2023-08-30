# -*- coding: utf-8 -*-
import time
import pandas as pd
from pathlib import Path

excel_dict = {'path': Path(__file__).parent.parent.parent.joinpath('excel')  # 默认excel路径
              }
cfg_file = Path(__file__).parent.parent / 'cfg' / 'setting_lib.py'
if cfg_file.exists():
    from cfg.setting_lib import excel_dict


def write2excel(df_dict, filename='temp', path=None, index=0):
    # 把df列表写入单个Excel文件
    engine = 'xlsxwriter'
    if not path:
        path = excel_dict.get('path')
        if not path.exists():
            path.mkdir(parents=True, exist_ok=True)
    try:
        writer = pd.ExcelWriter(path / f'{filename}.xlsx', engine=engine)
    except PermissionError:
        time_str = time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()))
        writer = pd.ExcelWriter(path / f'{filename}_{time_str}.xlsx', engine=engine)

    # 如果是dataframe list的话，直接按照序号写入
    if isinstance(df_dict, list):
        for i, df in enumerate(df_dict):
            df.to_excel(writer, f'sheet{i}', index=index)

    # 如果是字典，字典的索引必须是字符串，字典的值是dataframe
    elif isinstance(df_dict, dict):
        for k, v in df_dict.items():
            v.to_excel(writer, k, index=index)

    # pd.Dataframe 直写文件
    elif isinstance(df_dict, pd.DataFrame):
        df_dict.to_excel(writer, index=index)
    else:
        print(type(df_dict), 'can not write')

    # 样式水平居中
    wb = writer.book
    fm = wb.add_format({'valign': 'vcenter'})
    for sht in writer.sheets:
        # 行高和列宽都是22
        writer.sheets[sht].set_column('A:AAA', 22)
        for i in range(1000):
            writer.sheets[sht].set_row(i, 22, fm)

    ## 保存文件
    writer.close()
