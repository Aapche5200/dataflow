
import pandas as pd


# df1 = pd.read_excel(r'/Users/apache/Downloads/tst1.xlsx')
# df2 = pd.read_excel(r'/Users/apache/Downloads/tst2.xlsx')
# df = [df1, df2]
# data=pd.concat(df)
# print(data)
# data.to_excel(r'/Users/apache/Downloads/tst.xlsx', index=False)

books = pd.read_excel(r'/Users/apache/Downloads/1.xlsx',usecols="A:N")  # 获取数据
data = [[24,
         6,
         '10.05',
         '10.07',
         '',
         '',
         105.0,
         39319534820,
         '',
         '',
         1,
         '115粉',
         '115粉*1+小样2196269+小样2297851+中样muf散粉1g',
         '59']]
new = pd.DataFrame(data,
                   columns=["序号",
                            "账号",
                            "日期",
                            "到库日期",
                            "空1",
                            "空2",
                            "金额",
                            "订单号",
                            "美国快递单号",
                            "状态",
                            "数量",
                            "主商品",
                            "全部商品",
                            "转运"])

df = [books, new]

dataA = pd.concat(df,sort=False)
print(dataA)

