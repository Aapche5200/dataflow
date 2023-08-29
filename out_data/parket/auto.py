import os
import sys
import subprocess

# 将输出重定向到文件中
# sys.stdout = open('log.txt', 'w')
# sys.stderr = open('error.txt', 'w')

# 确保程序当前目录正确
os.chdir(os.path.dirname(os.path.abspath(__file__)))

# 定义要执行的脚本文件名列表
script_list = ['regular_task.py']

for script in script_list:
    subprocess.Popen(['pythonw', script],
                     shell=True)
