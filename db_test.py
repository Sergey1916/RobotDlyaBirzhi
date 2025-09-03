import psycopg2
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Подключение к БД
conn = psycopg2.connect(
    dbname="робот",
    user="postgres",
    password="111",
    host="localhost",
    port="5432"
)
cur = conn.cursor()

# Чтение параметров
cur.execute("""
    SELECT
        start_timestamp,       -- bigint (UNIX time в секундах)
        end_timestamp,         -- bigint
        pair,                  -- TEXT
        direction_days,        -- INTEGER
        ema_length,            -- INTEGER
        min_ex_amp             -- FLOAT
    FROM bf_algo_sr2_ethusd_params
    WHERE id = 1
""")
params = cur.fetchone()

# Преобразование UNIX
start_timestamp = pd.to_datetime(params[0], unit='s')
end_timestamp = pd.to_datetime(params[1], unit='s')
pair = params[2]
direction_days = params[3]
ema_length = params[4]
min_ex_amp = params[5]

# UNIX секунды
start_unix = int(start_timestamp.timestamp())
end_unix = int(end_timestamp.timestamp())

print("Параметры из БД:")
print(f"start_timestamp = {start_timestamp}")
print(f"end_timestamp   = {end_timestamp}")
print(f"pair            = {pair}")
print(f"direction_days  = {direction_days}")
print(f"ema_length      = {ema_length}")
print(f"min_ex_amp      = {min_ex_amp}")

# Свечи
query = """
SELECT
    ct.mts AS timestamp,
    ct.open, ct.high, ct.low, ct.close
FROM bf_candles_ethusd ct
WHERE ct.mts >= %s - %s * 86400
  AND ct.mts <= %s
ORDER BY ct.mts;
"""
cur.execute(query, (start_unix, direction_days, end_unix))
rows = cur.fetchall()

candles = pd.DataFrame(rows, columns=["timestamp", "open", "high", "low", "close"])
candles["datetime"] = pd.to_datetime(candles["timestamp"], unit='s')

# EMA
def calculate_ema(series, length):
    ema_values = []
    k = 2 / (length + 1)
    for i in range(len(series)):
        if i < length * 2:
            ema_values.append(None)
        else:
            back_closes = series[i - (length * 2):i + 1].to_list()
            a = sum(back_closes) / length
            for j in range(length, -1, -1):
                a = back_closes[j] * k + a * (1 - k)
            ema_values.append(a)
    return ema_values

candles["ema"] = calculate_ema(candles["close"], ema_length)

# Индексы начала расчета
pre_start = start_timestamp - timedelta(days=direction_days)
start_calc_index = candles.index[candles['timestamp'] >= int(pre_start.timestamp())][0]
start_point_index = candles.index[candles['timestamp'] >= start_unix][0]

# Если EMA нет, ищем ближайшую
if pd.isna(candles.loc[start_calc_index, "ema"]):
    start_calc_index = candles[candles['ema'].notna()].index[0]

levels = []
ranges = []
sr_point = []
extremes = []
fixed_ex = None

# Первый временный экстремум
extremes.append({
    "trend_id": 0,
    "direction": None,
    "type": "max",
    "ex": candles.loc[start_calc_index, "ema"],
    "Cex": start_calc_index,
    "datetime": candles.loc[start_calc_index, "datetime"],
    "timestamp": candles.loc[start_calc_index, "timestamp"],
    "status": "inactive",
    "definition": "undefined"
})

# Основной цикл
for idx in range(start_calc_index + 1, len(candles)):
    if pd.isna(candles.loc[idx, "ema"]):
        continue

    current_ema = candles.loc[idx, "ema"]
    last_ex = extremes[-1]

    if last_ex["type"] == "max":
        if current_ema > last_ex["ex"]:
            last_ex.update({
                "ex": current_ema,
                "Cex": idx,
                "datetime": candles.loc[idx, "datetime"],
                "timestamp": candles.loc[idx, "timestamp"],
            })
        elif current_ema < last_ex["ex"] * (1 - min_ex_amp / 100):
            last_ex["status"] = "active"
            last_ex["definition"] = "defined"
            fixed_ex = last_ex.copy()
            extremes.append({
                "trend_id": last_ex["trend_id"],
                "direction": last_ex["direction"],
                "type": "min",
                "ex": current_ema,
                "Cex": idx,
                "datetime": candles.loc[idx, "datetime"],
                "timestamp": candles.loc[idx, "timestamp"],
                "status": "inactive",
                "definition": "undefined"
            })

    elif last_ex["type"] == "min":
        if current_ema < last_ex["ex"]:
            last_ex.update({
                "ex": current_ema,
                "Cex": idx,
                "datetime": candles.loc[idx, "datetime"],
                "timestamp": candles.loc[idx, "timestamp"],
            })
        elif current_ema > last_ex["ex"] * (1 + min_ex_amp / 100):
            last_ex["status"] = "active"
            last_ex["definition"] = "defined"
            fixed_ex = last_ex.copy()
            extremes.append({
                "trend_id": last_ex["trend_id"],
                "direction": last_ex["direction"],
                "type": "max",
                "ex": current_ema,
                "Cex": idx,
                "datetime": candles.loc[idx, "datetime"],
                "timestamp": candles.loc[idx, "timestamp"],
                "status": "inactive",
                "definition": "undefined"
            })
# Создание таблицы экстремумов
cur.execute("""
CREATE TABLE IF NOT EXISTS bf_sr2_1 (
    id SERIAL PRIMARY KEY,
    trend_id INT,
    direction TEXT,
    type TEXT,
    ex DOUBLE PRECISION,
    cex INT,
    datetime TIMESTAMP,
    timestamp BIGINT,
    status TEXT,
    definition TEXT
)
""")
conn.commit()

# Вставка в таблицу
to_insert = []
for ex in extremes:
    to_insert.append((
        ex["trend_id"],
        ex["direction"],
        ex["type"],
        float(ex["ex"]) if ex["ex"] is not None else None,
        ex["Cex"],
        ex["datetime"],
        int(ex["timestamp"]) if ex["timestamp"] is not None else None,
        ex["status"],
        ex["definition"]
    ))

insert_query = """
INSERT INTO bf_sr2_1
(trend_id, direction, type, ex, cex, datetime, timestamp, status, definition)
VALUES %s
"""
execute_values(cur, insert_query, to_insert)
conn.commit()

print("   ")
print(f"{len(to_insert)} экстремумов сохранено в БД.")
print("   ")
print("Fixed extremes:", fixed_ex)
print("   ")

