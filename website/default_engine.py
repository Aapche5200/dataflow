from db_con import DbCon


def get_default_engin():
    # 获取 DbCon 类中的第一个数据库连接
    if DbCon().engines:
        first_engine = next(iter(DbCon().engines.values()))
        query_first = ''' select '创建链接成功' '''
        result_first = first_engine.execute(query_first).fetchone()
        return first_engine
    else:
        print("没有数据库连接")

