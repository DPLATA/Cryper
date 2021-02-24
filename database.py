import pandas as pd
import requests
import sqlalchemy as sql
import sys
import numpy as np
from plotly.offline import download_plotlyjs, plot, iplot, init_notebook_mode
import plotly.graph_objs as go
import plotly.express as px

BITSO_API_URL = 'http://api.bitso.com/v3/trades/?book=btc_mxn'
MYSQL_LOCALHOST_URL = 'mysql+mysqldb://root:AsdfG95!@localhost/bitso_api'

# DATAFRAME CREATION FROM BITSO API DATA

response = requests.get(BITSO_API_URL)
json_response = response.json()
data = json_response['payload']
df = pd.DataFrame(data)

# QUERY VALUES

initial_q = 'INSERT INTO bitso_trades (book,created_at,amount,maker_side,price,tid) VALUES '

values_q = ','.join(['(\'{}\',\'{}\',{},\'{}\',{},{})'.format(
    row.book,
    row.created_at,
    row.amount,
    row.maker_side,
    row.price,
    row.tid) for idx, row in df.iterrows()
])

end_q = ' ON DUPLICATE KEY UPDATE book = values(book), created_at = values(created_at), amount = values(amount), maker_side = values(maker_side), price = values(price), tid = values(tid);'

def create_db_engine(url):
    engine = sql.create_engine(url)
    return engine

def create_query(i, v, e):
    query = i + v + e
    return query

engine = create_db_engine(MYSQL_LOCALHOST_URL)
query = create_query(initial_q, values_q, end_q)

engine.execute(query)

read_query = 'SELECT * FROM bitso_trades ORDER BY tid DESC LIMIT 5000;'

extraction = pd.read_sql(read_query, engine)
print(extraction.head())
print(extraction.tail())

extraction = extraction.reindex(index=extraction.index[::-1])
extraction.reset_index(inplace=True, drop=True)
print(extraction.head())
print(extraction.tail())

print(extraction.dtypes)

#fig = px.line(extraction.price)
#fig.show()

def sma(df, window):
    roll = df.rolling(window).mean()
    return roll.dropna()

extraction['ma18'] = sma(extraction.price, 18)
extraction['ma36'] = sma(extraction.price, 36)
extraction['ma200'] = sma(extraction.price, 200)
extraction['ma400'] = sma(extraction.price, 400)

print(extraction.head(36))

columns = ['price', 'ma18', 'ma36', 'ma200', 'ma400']

fig = go.Figure()
for column in columns:
    fig.add_trace(go.Scatter(x=extraction.index, y=extraction[column],
    mode='lines',
    name = column))
fig.update_layout(template='plotly_dark')

fig.show()