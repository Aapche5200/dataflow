# -*- coding: utf-8 -*-
import time
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from templates.pro_management.data_source import get_registered_systype


def create_db_engine_with_retry(url, max_retries, retry_interval):
    retry_count = 0
    while retry_count < max_retries:
        try:
            engine = create_engine(url)
            query = '''select '成功' '''
            result = engine.execute(query).fetchone()
            return engine
        except:
            time.sleep(retry_interval)
            print(retry_count)
            retry_count += 1

    raise OperationalError("Failed to establish database connection.")


class DbCon:
    def __init__(self):
        self.db_list = get_registered_systype()
        # 创建存储连接的字典
        self.engines = {}

        # 遍历数据库配置信息，根据不同类型添加相应前缀，并创建连接
        for name, config in self.db_list.items():
            for db_type, url in config.items():
                if "SqlServer" in db_type:
                    # SQL Server
                    engine_url = f"mssql+pymssql://{url}?charset=utf8"
                elif "Greenplum" in db_type:
                    # Greenplum
                    engine_url = f"postgresql+psycopg2://{url}"
                elif "Mysql" in db_type:
                    # Mysql
                    engine_url = f"mysql+pymysql://{url}"
                else:
                    # 其他数据库类型，假设这里是 MySQL
                    engine_url = f"mysql://{url}"  # 根据实际情况修改前缀

                # 创建连接
                if name not in self.engines:
                    self.engines[name] = create_db_engine_with_retry(engine_url,
                                                                     max_retries=3,
                                                                     retry_interval=20)




