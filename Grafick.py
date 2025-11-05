import psycopg2
import mysql.connector
import pandas as pd
import plotly.graph_objects as go

# Подключение к PostgreSQL
conn_pg = psycopg2.connect(
    dbname="robot",
    user="postgres",
    password="111",
    host="localhost",
    port="5432"
)
cur_pg = conn_pg.cursor()

# Старые экстремумы из PostgreSQL
cur_pg.execute("""
    SELECT timestamp, ex
    FROM bf_sr2_8
    WHERE ex IS NOT NULL
    ORDER BY timestamp ASC
""")
extremes_new = cur_pg.fetchall()

# Подключение к MySQL
conn_mysql = mysql.connector.connect(
    host="localhost",
    user="root",
    password="12345678",
    database="myydb"
)
cur_mysql = conn_mysql.cursor()

# Новые экстремумы из MySQL
cur_mysql.execute("""
    SELECT timestamp, ex
    FROM bf_sr2_extremes_ethusd
    WHERE ex IS NOT NULL
    ORDER BY timestamp ASC
""")
extremes_old = cur_mysql.fetchall()

# Закрываем соединения
cur_pg.close()
conn_pg.close()

cur_mysql.close()
conn_mysql.close()

# DataFrame для PostgreSQL
df_extremes_new = pd.DataFrame(extremes_new, columns=['timestamp', 'ex'])
df_extremes_new['Date'] = pd.to_datetime(df_extremes_new['timestamp'], unit='s')

# DataFrame для MySQL
df_extremes_old = pd.DataFrame(extremes_old, columns=['timestamp', 'ex'])
df_extremes_old['Date'] = pd.to_datetime(df_extremes_old['timestamp'], unit='s')

count_old = len(df_extremes_old)
count_new = len(df_extremes_new)

# График
fig = go.Figure()

fig.add_trace(go.Scatter(
    x=df_extremes_old['Date'],
    y=df_extremes_old['ex'],
    mode='markers',
    marker=dict(color='red', size=10),
    name=f'Старые Экстремумф ({count_old})'
))

fig.add_trace(go.Scatter(
    x=df_extremes_new['Date'],
    y=df_extremes_new['ex'],
    mode='markers',
    marker=dict(color='blue', size=4, symbol="diamond"),
    name=f'Экстремумы мои ({count_new})'
))

fig.update_layout(
    title=f'Сравнение всех экстремумов: старые = {count_old}, новые = {count_new}',
    xaxis_title='Время',
    yaxis_title='Цена',
    template='plotly_dark'
)

fig.show(renderer="browser")
