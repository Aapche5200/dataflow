import pandas as pd
import concurrent.futures
from db_con import DbCon
from sqlalchemy.sql import text

source_con = DbCon().engines['gp_test']
target_con = DbCon().engines['gp_test']

# 1. 获取数据库连接
if True:
    job_tablelist_source = 'ods_oa_formmain_2156'
    job_tablelist_target = 'tem_test1111'

    # 2. 获取源表列信息
    columns = source_con.execute(
        f'''
        SELECT 
        column_name, 
        data_type FROM 
        INFORMATION_SCHEMA.COLUMNS 
        WHERE table_name = '{job_tablelist_source}'
        ''').fetchall()

    # 3. 生成创建表的SQL语句
    create_table_sql = f"CREATE TABLE IF NOT EXISTS {job_tablelist_target} ("
    for column in columns:
        column_name = column.column_name
        create_table_sql += f"{column_name} TEXT,"  # 将所有字段设置为文本类型
    create_table_sql = create_table_sql.rstrip(",") + ");"

    if not target_con.execute(
            f'''
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_name = '{job_tablelist_target}'
            ''').fetchone():
        target_con.execute(create_table_sql)

    # 清空目标表数据
    target_con.execute(text(f"TRUNCATE TABLE {job_tablelist_target}"))

    # 4. 获取源表数据并批量插入目标表
    batch_size = 1000
    offset = 0

    # 执行数据插入
    target_columns = [column[0] for column in target_con.execute(
        f'''
        SELECT column_name 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE table_name = '{job_tablelist_target}'
        '''
    ).fetchall()]

    first_column = target_columns[0]

    # 定义一个函数来处理数据块并插入目标表
    def process_data_and_insert(datax):
        # 将所有列转换为文本类型
        data = datax.fillna('')
        data = data.astype('str')

        insert_query = (
            f'''
            INSERT INTO 
            {job_tablelist_target}  
            ({', '.join(target_columns)}) VALUES 
            ({','.join([':' + column_t for column_t in target_columns])})
            ''')

        values = [{column: row[column] for column in target_columns} for _, row in
                  data.iterrows()]
        target_con.execute(text(insert_query), values)


    while True:
        query = f'''
        SELECT * 
        FROM {job_tablelist_source}
        order by {first_column}
        OFFSET {offset} ROWS FETCH FIRST {batch_size} ROWS ONLY
        '''
        data = pd.read_sql(query, source_con)

        # 对特定列进行编码解码操作
        if '身份' in data.columns:
            data['身份'] = data['身份'].apply(
                lambda x: x.encode('latin1').decode('gbk'))

        # 如果没有数据了，退出循环
        if data.empty:
            break

        # 使用多线程并发处理数据块
        with concurrent.futures.ThreadPoolExecutor(max_workers=100) as executor:
            executor.map(process_data_and_insert, [data])

        offset += batch_size
        print(offset)

    job_result = 'T'
