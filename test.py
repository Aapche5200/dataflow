import json
import pandas as pd
from urllib.request import urlopen
from pandas.io.json import json_normalize

with urlopen('https://raw.githubusercontent.com/geohacker/india/master/state/india_state.geojson') as f:
    d = json.load(f)

df = json_normalize(d['features'])
print(df)