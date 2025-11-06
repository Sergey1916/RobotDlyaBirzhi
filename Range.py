import psycopg2
import pandas as pd
from datetime import datetime, timedelta

# подключение к БД
pg_conn = psycopg2.connect(
    dbname="robot",
    user="postgres",
    password="111",
    host="localhost",
    port="5432"
)
pg_conn.autocommit = True


def fetch_params(conn):
    query = "SELECT * FROM bf_algo_sr2_ethusd_params LIMIT 1"
    return pd.read_sql(query, conn).iloc[0].to_dict()
