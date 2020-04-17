from selenium import webdriver
import pandas as pd
import os

x = []

for h in range(1, 50):
    class Qiubai:
        def __init__(self):
            self.dr = webdriver.Chrome()
            self.dr.get(f'https://vacations.ctrip.com/list/around/r-huadong-118.html?p={h}')

        def getData(self):
            content_left = self.dr.find_element_by_class_name('main_col')
            content_center = content_left.find_elements_by_class_name('list_product_item')

            i = 1
            for total in content_center:
                x.append(total.text)
                print(x)
                i += 1

            self.quit()

        def quit(self):
            self.dr.quit()

        print(f"爬取第{h}页完成")


    Qiubai().getData()

df = pd.DataFrame(x)
df.columns = ['chufadi']
data = df['chufadi'].str.split('\n', expand=True)
writer = pd.ExcelWriter('华东数据' + '.xlsx')
data.to_excel(excel_writer=writer, sheet_name='文捷', index=False)

os.chdir(r'/Users/apache/Downloads/A-python')
writer.save()
