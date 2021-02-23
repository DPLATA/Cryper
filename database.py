import pandas as pd
import requests
import sqlalchemy as sql

response = requests.get('http://api.bitso.com/v3/trades/?book=btc_mxn')
json_response = response.json()
data = json_response["payload"]
df = pd.DataFrame(data)
print(df.columns)