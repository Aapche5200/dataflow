from selenium import webdriver
import pandas as pd
import os
from bs4 import BeautifulSoup
import urllib.request
import re

xx = []
yy = []

for h in range(0, 5):
    class Qiubai:
        def __init__(self):
            self.dr = webdriver.Chrome()
            self.dr.get(f'https://s.tuniu.com/search_complex/tours-hz-0-%E5%8D%8E%E4%B8%9C/{h}')

        def getData(self):
            content_left = self.dr.find_element_by_class_name('thelist')
            content_center = content_left.find_elements_by_class_name('theinfo')

            i = 1
            for total in content_center:
                xx.append(total.text)
                print(xx)
                i += 1

            div = self.dr.find_element_by_class_name('thelist')
            div1 = div.find_elements_by_class_name('clearfix')

            a = 1
            for hr in div1:
                yy.append(hr.get_property('href'))
                print(yy)
                a += 1

            self.quit()

        def quit(self):
            self.dr.quit()

    print(f"爬取第{h}页完成")

    Qiubai().getData()

df1 = pd.DataFrame(xx)
df2 = pd.DataFrame(yy)
writer = pd.ExcelWriter('华东途牛数据' + '.xlsx')
df1.to_excel(excel_writer=writer, sheet_name='文字', index=False)
df2.to_excel(excel_writer=writer, sheet_name='链接', index=False)

os.chdir(r'/Users/apache/Downloads/A-python')
writer.save()
