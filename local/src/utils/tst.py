# -*- coding: utf-8 -*-
import time
import psycopg2
import pandas as pd
import numpy as np
from sqlalchemy import create_engine


conn2 = create_engine('postgresql+psycopg2://data_etl:F3xgK#w9cO@lWbk$@10.20.121.145:5432/report')
conn = psycopg2.connect(user='data_etl',
                        password='F3xgK#w9cO@lWbk$',
                        host='10.20.121.145',
                        port=5432,
                        database='report_test')