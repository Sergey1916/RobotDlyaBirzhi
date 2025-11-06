import psycopg2
import pandas as pd
import datetime
import math
import json
import sys
from typing import Optional

pg_conn = psycopg2.connect(
    dbname="robot",
    user="postgres",
    password="111",
    host="localhost",
    port="5432"
)
pg_conn.autocommit = True

def create_range_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS bf_range (
                id SERIAL PRIMARY KEY,
                range_id INT,
                direction VARCHAR(20),
                def_ts BIGINT,
                def_date VARCHAR(20),
                start_ts BIGINT,
                start_date VARCHAR(20),
                le_date VARCHAR(20),
                end_ts BIGINT,
                end_date VARCHAR(20),
                pri TEXT,
                sec TEXT,
                create_reason VARCHAR(50)
            );
        """)
    print("Таблица bf_range готова.")

def fetch_params(conn) -> dict:
    q = "SELECT * FROM bf_algo_sr2_ethusd_params LIMIT 1"
    df = pd.read_sql_query(q, conn)
    if df.empty:
        raise RuntimeError("bf_algo_sr2_ethusd_params пустая или не найдена")
    return df.iloc[0].to_dict()


def fetch_extremes(conn) -> pd.DataFrame:
    q = """
        SELECT id, timestamp, direction, type, ex, cex, datetime
        FROM bf_sr2_8
        WHERE status = 'active'
        ORDER BY timestamp ASC
    """
    return pd.read_sql_query(q, conn)


def fetch_levels(conn, include_all: bool = True) -> pd.DataFrame:
    if include_all:
        q = """
            SELECT id, level_id, type, kind, status, level_value,
                   top, bottom, ex_start, ex_start_date,
                   start_ts, start_date, end_ts, end_date
            FROM bf_levels
            ORDER BY start_ts ASC NULLS LAST
        """
    else:
        q = """
            SELECT id, level_id, type, kind, status, level_value,
                   top, bottom, ex_start, ex_start_date,
                   start_ts, start_date, end_ts, end_date
            FROM bf_levels
            WHERE status = 'active'
            ORDER BY start_ts ASC NULLS LAST
        """
    return pd.read_sql_query(q, conn)


def finalize_current_level(conn, level_db_id: Optional[int], ts: int):
    """
    Обновляет существующий уровень: ставит end_ts, end_date, complete_reason='activation', status='inactive'
    """
    if level_db_id is None:
        return
    end_date = datetime.datetime.fromtimestamp(int(ts)).strftime("%Y-%m-%d %H:%M:%S")
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE bf_levels
            SET end_ts = %s,
                end_date = %s,
                complete_reason = 'activation',
                status = 'inactive'
            WHERE id = %s
        """, (ts, end_date, level_db_id))
    conn.commit()
    print(f"Уровень id={level_db_id} завершён (activation).")

# Формирование Pri JSON
def build_pri_obj(current_level_row: dict, level_ex: dict, outlier_ex: Optional[dict], ts: int, margin: float):

    ex = outlier_ex if outlier_ex is not None else level_ex
    if ex is None:
        raise RuntimeError("Нет экстремума (level_ex/outlier_ex) для создания pri")

    level_value = float(ex["ex"])
    top = level_value * (1 + margin / 100.0)
    bottom = level_value * (1 - margin / 100.0)

    obj_id = int(current_level_row.get("id", 0)) if current_level_row.get("id") is not None else 0
    level_id = int(current_level_row.get("level_id", 0)) if current_level_row.get("level_id") is not None else 0

    lvl_type = current_level_row.get("type") or "support"
    lvl_kind = current_level_row.get("kind") or "up"

    ex_start = int(ex.get("id") or ex.get("cex") or 0)
    ex_start_date = datetime.datetime.fromtimestamp(int(ex["timestamp"])).strftime("%m/%d/%Y %H:%M:%S")

    start_date = datetime.datetime.fromtimestamp(int(ts)).strftime("%m/%d/%Y %H:%M:%S")

    pri_obj = {
        "id": obj_id,
        "level_id": level_id,
        "type": lvl_type,
        "kind": lvl_kind,
        "status": "active",
        "level_value": level_value,
        "top": top,
        "bottom": bottom,
        "start_ts": int(ts),
        "start_date": start_date,
        "end_ts": int(ts),
        "end_date": start_date,
        "create_reason": "activation",
        "complete_reason": "cancel",
        "ex_start": ex_start,
        "ex_start_date": ex_start_date,
        "rules": ["shift", "cross_new_range"]
    }
    return pri_obj

def save_range_record(conn, data: dict):
    pri_data = data.get("pri")
    if isinstance(pri_data, (dict, list)):
        pri_data = json.dumps(pri_data, ensure_ascii=False)
    sec_data = data.get("sec")
    if isinstance(sec_data, (dict, list)):
        sec_data = json.dumps(sec_data, ensure_ascii=False)

    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO bf_range (
                range_id, direction, def_ts, def_date,
                start_ts, start_date, le_date,
                end_ts, end_date, pri, sec, create_reason
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            data.get("range_id"),
            data.get("direction"),
            data.get("def_ts"),
            data.get("def_date"),
            data.get("start_ts"),
            data.get("start_date"),
            data.get("le_date"),
            data.get("end_ts"),
            data.get("end_date"),
            pri_data,
            sec_data,
            data.get("create_reason")
        ))
    conn.commit()

# Основная логика определения
def detect_range(conn):
    print("Загружаем параметры и данные.")
    params = fetch_params(conn)

    # параметры
    margin = float(params.get("margin", 0))
    define_hours = float(params.get("define_hours", 1))
    ema_length = int(params.get("ema_length", 20) if params.get("ema_length") is not None else 20)
    l2b_cond_mult = float(params.get("l2b_cond_mult", 0))
    prl2min_diff = float(params.get("prl2min_diff", 0))
    mwp = float(params.get("mwp", 0))
    outlier_enabled = bool(params.get("Outlier", params.get("outlier", False)))
    outlier_min_diff = float(params.get("outlier_min_diff", 0))
    outlier_max_hours = float(params.get("outlier_max_hours", 0))

    df_ext = fetch_extremes(conn)
    df_levels = fetch_levels(conn, include_all=True)

    if df_ext.empty:
        print("Нет экстремумов (active) в bf_sr2_8")
        return

    # сортировка и EMA по ex
    df_ext = df_ext.sort_values("timestamp").reset_index(drop=True)
    df_ext["ema"] = df_ext["ex"].ewm(span=ema_length, adjust=False).mean()

    # словарь экстремумов по id
    ext_by_id = {}
    if "id" in df_ext.columns:
        for _, r in df_ext.iterrows():
            try:
                ext_by_id[int(r["id"])] = r.to_dict()
            except Exception:
                pass

    # состояние и счётчики
    range_start_point = None
    range_start_idx = None
    range_cancelled = False

    seq_range_id = 0
    ranges_created = 0
    pri_created = 0

    print("Старт обхода экстремумов")

    for idx, row in df_ext.iterrows():
        current_ts = int(row["timestamp"])
        ema = float(row["ema"])
        direction = str(row["direction"]).lower()
        ex_val = float(row["ex"])

        if "kind" in df_levels.columns:
            subset = df_levels[df_levels["kind"].astype(str).str.lower() == direction]
        else:
            subset = df_levels.copy()

        if subset.empty:
            subset = df_levels.copy()
        if subset.empty:
            continue

        if "start_ts" in subset.columns:
            subset_local = subset.copy()
            subset_local["start_ts_fill"] = subset_local["start_ts"].fillna(10**18)
            nearest_idx = (subset_local["start_ts_fill"] - current_ts).abs().argsort()[:1]
            level_row = subset_local.iloc[nearest_idx[0]].to_dict()
        else:
            level_row = subset.iloc[0].to_dict()

        # границы уровня
        try:
            cl_top = float(level_row.get("top")) if level_row.get("top") is not None else float("nan")
        except:
            cl_top = float("nan")
        try:
            cl_bottom = float(level_row.get("bottom")) if level_row.get("bottom") is not None else float("nan")
        except:
            cl_bottom = float("nan")

        # проверка пересечения EMA и уровня
        crossed = False
        if direction == "down":
            if not math.isnan(cl_bottom) and ema < cl_bottom:
                crossed = True
        else:  # up
            if not math.isnan(cl_top) and ema > cl_top:
                crossed = True

        if crossed:
            if range_start_point is None:
                range_start_point = current_ts
                range_start_idx = idx
                range_cancelled = False
                # не продолжаем на той же итерации — ждём следующей точки чтобы пройти define_hours
                continue

        # если уже есть стартовая точка и не отменено
        if range_start_point is not None and not range_cancelled:
            time_diff_hours = (current_ts - range_start_point) / 3600.0
            if time_diff_hours > define_hours:
                # дополнительные проверки

                # 1) level_ex — экстремум, определивший текущий уровень
                level_ex = None
                ex_start_val = level_row.get("ex_start")
                if ex_start_val and isinstance(ex_start_val, (int, float)) and int(ex_start_val) in ext_by_id:
                    level_ex = ext_by_id[int(ex_start_val)]
                else:
                    if level_row.get("start_ts") is not None:
                        s_ts = int(level_row.get("start_ts"))
                        nearest_ext = df_ext.iloc[(df_ext["timestamp"] - s_ts).abs().argsort()[:1]].iloc[0]
                        level_ex = nearest_ext.to_dict()
                    else:
                        level_ex = row.to_dict()

                le_ts = int(level_ex.get("timestamp", current_ts))

                # 2) ищем экстремумы противоположного типа между level_ex и current
                post_level_ex = None
                if level_ex.get("type") in ("max", "min"):
                    opp_type = "min" if level_ex.get("type") == "max" else "max"
                    if le_ts < current_ts:
                        between = df_ext[(df_ext["timestamp"] > le_ts) & (df_ext["timestamp"] <= current_ts)]
                    else:
                        between = pd.DataFrame(columns=df_ext.columns)
                    opps = between[between["type"] == opp_type]
                    if not opps.empty:
                        if direction == "down":
                            # для down — берем максимальный
                            row_ple = opps.loc[opps["ex"].idxmax()]
                            post_level_ex = row_ple.to_dict()
                        else:
                            row_ple = opps.loc[opps["ex"].idxmin()]
                            post_level_ex = row_ple.to_dict()

                # 3) проверка EMA vs post_level_ex
                cancel_due_to_post_ex = False
                if post_level_ex is not None:
                    ple_ts = int(post_level_ex.get("timestamp"))
                    ple_segment = df_ext[(df_ext["timestamp"] >= ple_ts) & (df_ext["timestamp"] <= current_ts)]
                    if not ple_segment.empty:
                        if direction == "up":
                            min_ema = ple_segment["ema"].min()
                            if min_ema < float(post_level_ex["ex"]):
                                cancel_due_to_post_ex = True
                        else:
                            max_ema = ple_segment["ema"].max()
                            if max_ema > float(post_level_ex["ex"]):
                                cancel_due_to_post_ex = True

                if cancel_due_to_post_ex:
                    range_start_point = None
                    range_start_idx = None
                    range_cancelled = True
                    continue

                # 4) Outlier (только для down)
                outlier_detected = False
                outlier_ex = None
                pre_outlier_ts = None
                post_outlier_ts = None

                if outlier_enabled and direction == "down":
                    mins = df_ext[(df_ext["timestamp"] > le_ts) & (df_ext["timestamp"] <= current_ts) & (df_ext["type"] == "min")]
                    if not mins.empty:
                        min_row = mins.loc[mins["ex"].idxmin()]
                        outlier_candidate = min_row.to_dict()
                        try:
                            level_ex_val = float(level_ex["ex"])
                            out_val = float(outlier_candidate["ex"])
                            pct = abs(level_ex_val - out_val) * 100.0 / (level_ex_val if level_ex_val != 0 else 1e-9)
                            if pct > outlier_min_diff:
                                # ищем pre_outlier_ts и post_outlier_ts relative to level_ex index
                                level_idx = (df_ext["timestamp"] - le_ts).abs().argsort()[:1][0]
                                pre_ts = None
                                for j in range(level_idx, -1, -1):
                                    if df_ext.loc[j, "ema"] > out_val:
                                        pre_ts = int(df_ext.loc[j, "timestamp"])
                                        break
                                post_ts = None
                                for j in range(level_idx, int(idx) + 1):
                                    if df_ext.loc[j, "ema"] > out_val:
                                        post_ts = int(df_ext.loc[j, "timestamp"])
                                        break
                                if pre_ts is not None and post_ts is not None and (post_ts - pre_ts) < (outlier_max_hours * 3600.0):
                                    outlier_detected = True
                                    outlier_ex = outlier_candidate
                                    pre_outlier_ts = pre_ts
                                    post_outlier_ts = post_ts
                        except Exception:
                            outlier_detected = False

                # 5) фиксируем Range
                seq_range_id += 1
                range_id_local = seq_range_id
                def_ts = current_ts
                def_date = datetime.datetime.utcfromtimestamp(current_ts).strftime("%Y-%m-%d %H:%M:%S")

                # сформировать pri JSON
                try:
                    pri_obj = build_pri_obj(level_row, level_ex, outlier_ex if outlier_detected else None, current_ts, margin)
                    pri_created += 1
                except Exception as e:
                    print("Ошибка формирования pri:", e)
                    pri_obj = None

                # sec оставляем простым пока
                sec_info = f"ema={round(ema, 8)}"

                data = {
                    "range_id": range_id_local,
                    "direction": direction,
                    "def_ts": def_ts,
                    "def_date": def_date,
                    "start_ts": range_start_point,
                    "start_date": datetime.datetime.utcfromtimestamp(range_start_point).strftime("%Y-%m-%d %H:%M:%S"),
                    "le_date": def_date,
                    "end_ts": current_ts,
                    "end_date": def_date,
                    "pri": [pri_obj] if pri_obj is not None else None,
                    "sec": sec_info,
                    "create_reason": "define_hours"
                }

                # сохранить range
                save_range_record(conn, data)
                ranges_created += 1

                # завершить текущий уровень и сформировали pri JSON
                try:
                    finalize_current_level(conn, int(level_row.get("id")) if level_row.get("id") is not None else None, current_ts)
                except Exception as e:
                    print("Ошибка:", e)

                # сброс состояния
                range_start_point = None
                range_start_idx = None
                range_cancelled = False

    print(f"Добавлено записей в bf_range: {ranges_created}, сформировано pri JSON: {pri_created}")


# Запуск
if __name__ == "__main__":
    try:
        create_range_table(pg_conn)
        detect_range(pg_conn)
    except Exception as e:
        print("Ошибка выполнения:", e)
        sys.exit(1)

