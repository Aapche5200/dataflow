# -*- coding: utf-8 -*-
import json
import time
from pathlib import Path


def write_json(_dict):
    file_path = Path(__file__).parent.parent / 'cfg'
    if not file_path.exists():
        file_path.mkdir(parents=True, exist_ok=True)
    with open(file_path / 'serialize.json', 'w') as f:
        json.dump(_dict, f)


def read_json():
    with open(Path(__file__).parent.parent / 'cfg' / 'serialize.json', 'r') as f:
        return json.load(f)


def update_json(**kwargs):
    try:
        _dict = read_json()
    except:
        _dict = {}
    _dict.update(kwargs)
    write_json(_dict)


def update_json_time(type):
    job_time = time.time()
    job_time_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(job_time))
    update_json(**{type: job_time, type + '_str': job_time_str})
