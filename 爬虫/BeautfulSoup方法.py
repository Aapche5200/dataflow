from bs4 import BeautifulSoup
import urllib.request
import re

html_doc = "https://vacations.ctrip.com/list/around/r--118.html?p=1"
req = urllib.request.Request(html_doc)
webpage = urllib.request.urlopen(req)
html = webpage.read()

soup = BeautifulSoup(html, 'html.parser')

for k in soup.find_all('div'):
    # print(k)
    print(k.get('string'))

