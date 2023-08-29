import pandas as pd
import re

# 示例DataFrame
df = pd.DataFrame({
    'A': ['A, B', 'C.', 'D/', 'E\\', 'F', 'G, H '],
    'B': ['I', 'J,', 'K.', 'L\\', 'M', 'N, O '],
    'C': [', ', ' . ', '/J，K。L\\', '/', '。', '\\'],
    'D': ['P', '/\\', '/\R', 'S', 'T', 'U']
})
# 定义正则表达式规则
pattern = r'^[.。,/\\]+$'

# 处理每个列的数据
df_change = df.applymap(lambda x: '' if re.match(pattern, str(x).strip()) else x)

print(df)
