import pathlib
import base64


def read_enc_info(code):
    return base64.b64decode(code.encode()).decode()


# 读取配置文件
IS_ONLINE = True
from utils.setting_env import env_dict

parent_cur_dir = pathlib.Path(pathlib.Path(__file__).absolute()).parent
file_path = parent_cur_dir.joinpath('setting_env_test.py')
if file_path.exists():
    from utils.setting_env_test import env_dict

    IS_ONLINE = False

# 读取设定参数
env_info = env_dict.get('env_info')
env_info = {k: read_enc_info(v) for k, v in env_info.items()}
logpath = env_dict.get('logpath')
schedule_job_dict = env_dict.get('schedule_job_dict')
debug = env_dict.get('debug')
db_state = env_dict.get('db_state')
