import psycopg2
import pandas as pd
import plotly.graph_objects as go

limit = 300
start_ts = 1522332000  # Начало графика (29 марта 2018)

conn = psycopg2.connect(
    dbname="robot",
    user="postgres",
    password="111",
    host="localhost",
    port="5432"
)
cur = conn.cursor()

# Получение параметров
cur.execute("""
    SELECT
        start_timestamp,
        end_timestamp,
        pair,
        direction_days,
        ema_length,
        min_ex_amp
    FROM bf_algo_sr2_ethusd_params
    WHERE id = 1
""")
params = cur.fetchone()

start_timestamp = params[0]
end_timestamp = params[1]
pair = params[2]
direction_days = params[3]
ema_length = params[4]
min_ex_amp = params[5]

# Выборка свечей
cur.execute("""
    SELECT mts, open, high, low, close
    FROM bf_candles_ethusd
    WHERE mts >= %s
    ORDER BY mts ASC
    LIMIT %s
""", (start_ts, limit))
candles = cur.fetchall()

# Выборка новы экстремумов в диапазоне свечей
cur.execute("""
    SELECT timestamp, ex
    FROM bf_sr2_4
    WHERE ex IS NOT NULL
      AND timestamp BETWEEN %s AND %s
    ORDER BY timestamp ASC
""", (candles[0][0], candles[-1][0]))
extremes_new = cur.fetchall()

# Выборка старых экстремумов в диапазоне свечей
cur.execute("""
    SELECT timestamp, ex
    FROM bf_sr2_sit_extremes_ethusd
    WHERE ex IS NOT NULL
      AND timestamp BETWEEN %s AND %s
    ORDER BY timestamp ASC
""", (candles[0][0], candles[-1][0]))
extremes_old = cur.fetchall()

cur.close()
conn.close()

df_candles = pd.DataFrame(candles, columns=['mts', 'Open', 'High', 'Low', 'Close'])
df_candles['Date'] = pd.to_datetime(df_candles['mts'], unit='s')

df_candles["EMA"] = df_candles["Close"].ewm(span=ema_length, adjust=False).mean()

df_extremes_old = pd.DataFrame(extremes_old, columns=['timestamp', 'ex'])
df_extremes_old['Date'] = pd.to_datetime(df_extremes_old['timestamp'], unit='s')

df_extremes_new = pd.DataFrame(extremes_new, columns=['timestamp', 'ex'])
df_extremes_new['Date'] = pd.to_datetime(df_extremes_new['timestamp'], unit='s')

# График
fig = go.Figure()

# Свечи
fig.add_trace(go.Candlestick(
    x=df_candles['Date'],
    open=df_candles['Open'],
    high=df_candles['High'],
    low=df_candles['Low'],
    close=df_candles['Close'],
    name='Свечи'
))

# Экстремумы
fig.add_trace(go.Scatter(
    x=df_extremes_old['Date'],
    y=df_extremes_old['ex'],
    mode='markers',
    marker=dict(color='red', size=20),
    name='Экстремумы старые'
))

fig.add_trace(go.Scatter(
    x=df_extremes_new['Date'],
    y=df_extremes_new['ex'],
    mode='markers',
    marker=dict(color='blue', size=16, symbol="diamond"),
    name='Экстремумы новые'
))
# EMA
fig.add_trace(go.Scatter(
    x=df_candles['Date'],
    y=df_candles['EMA'],
    mode='lines',
    line=dict(color='yellow', width=2),
    name=f'EMA {ema_length}'
))

fig.update_layout(
    title=f'Свечи + экстремумы + EMA {ema_length} + {pair}',
    xaxis_title='Время',
    yaxis_title='Цена',
    template='plotly_dark'
)

fig.show(renderer="browser")
