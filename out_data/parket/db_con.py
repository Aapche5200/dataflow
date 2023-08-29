# -*- coding: utf-8 -*-
import time
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError


def create_db_engine_with_retry(url, max_retries=3, retry_interval=1):
    retry_count = 0
    while retry_count < max_retries:
        try:
            engine = create_engine(url)
            return engine
        except OperationalError:
            print(f"Connection failed. Retrying ({retry_count + 1}/{max_retries})...")
            time.sleep(retry_interval)
            retry_count += 1

    raise OperationalError("Failed to establish database connection.")


class DbCon:
    con_oa = create_db_engine_with_retry(
        'mssql+pymssql://sjfx:5VvXTWS%nfbPRex1Sb@10.20.120.114:1433/ABV5?timeout=60', max_retries=3, retry_interval=1)
    con_gp = create_db_engine_with_retry(
        'postgresql+psycopg2://python_etl:xFFUfuXDA4e5CVC1@10.20.121.145:5432/report?connect_timeout=60', max_retries=3,
        retry_interval=1)
    con_quoter = create_db_engine_with_retry(
        'postgresql+psycopg2://ltc_proconfig:gr4*#R7&Ej%*s#$6M3kY@10.20.121.113:32432/ltc_proconfig?connect_timeout=60',
        max_retries=3, retry_interval=1)
    con_erp = create_db_engine_with_retry('mssql+pymssql://sjfx:A20220307a*@10.20.120.85:50932/DBAPPSecurty?timeout=60',
                                          max_retries=3, retry_interval=1)
