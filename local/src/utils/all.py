# -*- coding: utf-8 -*-
from utils.read_sql_server import read_ss_df
from utils.read_sql_ck import read_ck_df
from lib.excel import write2excel
import pandas as pd
from pathlib import Path
from utils.public_object import con_dict


def read_sql(sql, db_code='ss'):
    con = con_dict.get(db_code).connect()
    df = pd.read_sql(sql, server_con, coerce_float=False)
    return df
