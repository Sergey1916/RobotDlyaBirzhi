import psycopg2
import pandas as pd
import datetime
import math
import json
import sys
from typing import Optional, List, Dict

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
            FROM bf_levels1
            ORDER BY start_ts ASC NULLS LAST
        """
    else:
        q = """
            SELECT id, level_id, type, kind, status, level_value,
                   top, bottom, ex_start, ex_start_date,
                   start_ts, start_date, end_ts, end_date
            FROM bf_levels1
            WHERE status = 'active'
            ORDER BY start_ts ASC NULLS LAST
        """
    return pd.read_sql_query(q, conn)


def finalize_current_level(conn, level_db_id: Optional[int], ts: int):
    if level_db_id is None:
        return
    end_date = datetime.datetime.fromtimestamp(int(ts)).strftime("%Y-%m-%d %H:%M:%S")
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE bf_levels1
            SET end_ts = %s,
                end_date = %s,
                complete_reason = 'activation',
                status = 'inactive'
            WHERE id = %s
        """, (ts, end_date, level_db_id))
    conn.commit()
    print(f"Уровень id={level_db_id} завершён (activation).")


# Формирование pri JSON

def build_pri_obj(current_level_row: dict, level_ex: dict, outlier_ex: Optional[dict], ts: int, margin: float) -> Dict:
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


# Вспомогательная: получить предыдущий range (последнюю запись в bf_range)

def get_last_range_from_db(conn) -> Optional[dict]:
    with conn.cursor() as cur:
        cur.execute("SELECT range_id, direction, pri, sec, start_ts, end_ts FROM bf_range ORDER BY id DESC LIMIT 1")
        row = cur.fetchone()
        if not row:
            return None
        return {
            "range_id": row[0],
            "direction": row[1],
            "pri": row[2],
            "sec": row[3],
            "start_ts": row[4],
            "end_ts": row[5]
        }



def candidate_from_ext(ex_row: dict, margin: float, source_label: str) -> Dict:
    val = float(ex_row["ex"])
    return {
        "source": source_label,
        "level_value": val,
        "top": val * (1 + margin / 100.0),
        "bottom": val * (1 - margin / 100.0),
        "ex_start": int(ex_row.get("id") or 0),
        "ex_start_date": datetime.datetime.fromtimestamp(int(ex_row["timestamp"])).strftime("%m/%d/%Y %H:%M:%S")
    }



# Поиск pre_level_ex (пункт 3)

def find_pre_level_ex(df_ext: pd.DataFrame, level_ex: dict, post_level_ex: Optional[dict], direction: str) -> Optional[dict]:
    # по описанию ТЗ: для up -> искать last min < post_level_ex, потом last max перед ним; для down — зеркально.
    try:
        le_ts = int(level_ex.get("timestamp"))
        before = df_ext[df_ext["timestamp"] < le_ts]
        if before.empty:
            return None
        if direction == "up":
            if post_level_ex is not None:
                post_val = float(post_level_ex["ex"])
                cand_mins = before[(before["type"] == "min") & (before["ex"] < post_val)]
                if cand_mins.empty:
                    return None
                pre_level_min = cand_mins.iloc[-1]
                before_min_ts = int(pre_level_min["timestamp"])
                cand_maxs = before[(before["type"] == "max") & (before["timestamp"] < before_min_ts)]
                pre_level_max = cand_maxs.iloc[-1] if not cand_maxs.empty else None
                if pre_level_max is not None and float(pre_level_max["ex"]) < post_val:
                    chosen = pre_level_max.to_dict()
                else:
                    chosen = pre_level_min.to_dict()
                return chosen
            else:
                opp = "min"
                cand = before[before["type"] == opp]
                if cand.empty:
                    return None
                return cand.iloc[-1].to_dict()
        else:  # down
            if post_level_ex is not None:
                post_val = float(post_level_ex["ex"])
                cand_maxs = before[(before["type"] == "max") & (before["ex"] > post_val)]
                if cand_maxs.empty:
                    return None
                pre_level_max = cand_maxs.iloc[-1]
                before_max_ts = int(pre_level_max["timestamp"])
                cand_mins = before[(before["type"] == "min") & (before["timestamp"] < before_max_ts)]
                pre_level_min = cand_mins.iloc[-1] if not cand_mins.empty else None
                if pre_level_min is not None and float(pre_level_min["ex"]) > post_val:
                    chosen = pre_level_min.to_dict()
                else:
                    chosen = pre_level_max.to_dict()
                return chosen
            else:
                opp = "max"
                cand = before[before["type"] == opp]
                if cand.empty:
                    return None
                return cand.iloc[-1].to_dict()
    except Exception:
        return None


# Формирование sec

def build_sec(
    conn,
    df_ext: pd.DataFrame,
    direction: str,
    level_ex: dict,
    level_row: dict,
    post_level_ex: Optional[dict],
    margin: float,
    l2b_cond_mult: float,
    prl2min_diff: float,
    mwp: float,
) -> Optional[Dict]:
    secondary_levels: List[Dict] = []

    # 1) post_level_ex candidate
    if post_level_ex is not None:
        secondary_levels.append(candidate_from_ext(post_level_ex, margin, "post_level_ex"))

    # 2) previous range border candidate (если есть last range)
    last_range = get_last_range_from_db(conn)
    if last_range is not None:
        try:
            last_dir = (last_range.get("direction") or "").lower()
            last_border = None
            # for new up: last_border = primary previous if prev was up, else secondary if prev was down
            if direction == "up":
                if last_dir == "up":
                    last_border = json.loads(last_range.get("pri") or "null")
                    if isinstance(last_border, list):
                        last_border = last_border[0]
                else:
                    last_border = json.loads(last_range.get("sec") or "null")
            else:  # direction == 'down'
                if last_dir == "up":
                    last_border = json.loads(last_range.get("sec") or "null")
                else:
                    last_border = json.loads(last_range.get("pri") or "null")
            if last_border:
                last_val = float(last_border.get("level_value", 0))
                post_val = float(post_level_ex["ex"]) if post_level_ex is not None else last_val
                if direction == "up":
                    if last_val < post_val:
                        cond_left = abs(last_val - post_val) * l2b_cond_mult
                        cond_right = abs(post_val - float(level_row.get("level_value", level_ex.get("ex"))))
                        if cond_left < cond_right:
                            secondary_levels.append({
                                "source": "last_border",
                                "level_value": last_val,
                                "top": last_val * (1 + margin / 100.0),
                                "bottom": last_val * (1 - margin / 100.0),
                                "ex_start": int(last_border.get("ex_start", 0)),
                                "ex_start_date": last_border.get("ex_start_date", "")
                            })
                else:  # down
                    if last_val > post_val:
                        secondary_levels.append({
                            "source": "last_border",
                            "level_value": last_val,
                            "top": last_val * (1 + margin / 100.0),
                            "bottom": last_val * (1 - margin / 100.0),
                            "ex_start": int(last_border.get("ex_start", 0)),
                            "ex_start_date": last_border.get("ex_start_date", "")
                        })
        except Exception:
            pass

    # 3) pre_level_ex candidate
    pre = find_pre_level_ex(df_ext, level_ex, post_level_ex, direction)
    if pre is not None:
        if post_level_ex is not None:
            cond_left = abs(float(pre["ex"]) - float(post_level_ex["ex"])) * l2b_cond_mult
            cond_right = abs(float(post_level_ex["ex"]) - float(level_ex["ex"]))
            if cond_left < cond_right:
                secondary_levels.append(candidate_from_ext(pre, margin, "pre_level_ex"))
        else:
            secondary_levels.append(candidate_from_ext(pre, margin, "pre_level_ex"))

    if not secondary_levels:
        return None

    if direction == "down":
        secondary_levels = sorted(secondary_levels, key=lambda x: x["level_value"])
    else:
        secondary_levels = sorted(secondary_levels, key=lambda x: x["level_value"], reverse=True)

    # последний элемент
    sec = secondary_levels[-1].copy()
    sec["status"] = "active"
    sec["start_ts"] = int(level_ex["timestamp"])
    sec["start_date"] = datetime.datetime.fromtimestamp(int(level_ex["timestamp"])).strftime("%m/%d/%Y %H:%M:%S")
    sec["create_reason"] = "range"
    sec["rules"] = ["shift", "cross_turn"]
    sec["type"] = "resistance" if direction == "up" else "support"
    sec["kind"] = "main"

    # проверка mwp
    # top_border_value - pri_n для up, sec_n для down
    sec["narrow"] = False
    return sec


# Сохранение Range в bf_range

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



# Основная функция detect_range

def detect_range(conn):
    print("Загружаем параметры и данные.")
    params = fetch_params(conn)

    # параметры
    margin = float(params.get("margin", 0))
    define_hours = float(params.get("define_hours", 1))
    ema_length = int(params.get("ema_length", 20) if params.get("ema_length") is not None else 20)
    l2b_cond_mult = float(params.get("l2b_cond_mult", 1))
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

    df_ext = df_ext.sort_values("timestamp").reset_index(drop=True)
    df_ext["ema"] = df_ext["ex"].ewm(span=ema_length, adjust=False).mean()

    ext_by_id = {}
    if "id" in df_ext.columns:
        for _, r in df_ext.iterrows():
            try:
                ext_by_id[int(r["id"])] = r.to_dict()
            except Exception:
                pass

    # состояние
    range_start_point = None
    range_start_idx = None
    range_cancelled = False

    seq_range_id = 0
    ranges_created = 0
    pri_created = 0

    print("Старт обхода экстремумов...")

    for idx, row in df_ext.iterrows():
        current_ts = int(row["timestamp"])
        ema = float(row["ema"])
        direction = str(row["direction"]).lower()
        ex_val = float(row["ex"])

        # выбор текущего уровня: использовать type (support/resistance)
        if direction == "down":
            subset = df_levels[df_levels["type"].astype(str).str.lower() == "support"]
        else:
            subset = df_levels[df_levels["type"].astype(str).str.lower() == "resistance"]

        if subset.empty:
            # fallback — используем все уровни
            subset = df_levels.copy()
        if subset.empty:
            continue

        # ближайшее к start_ts
        if "start_ts" in subset.columns:
            subset_local = subset.copy()
            subset_local["start_ts_fill"] = subset_local["start_ts"].fillna(10**18)
            nearest_idx = (subset_local["start_ts_fill"] - current_ts).abs().argsort().values[0]
            level_row = subset_local.iloc[nearest_idx].to_dict()

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
                continue

        if range_start_point is not None and not range_cancelled:
            time_diff_hours = (current_ts - range_start_point) / 3600.0
            if time_diff_hours > define_hours:
                # дополнительные проверки
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

                # post_level_ex поиск (противоположный type между level_ex и текущей точкой)
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
                            # для down — максимальный
                            row_ple = opps.loc[opps["ex"].idxmax()]
                            post_level_ex = row_ple.to_dict()
                        else:
                            row_ple = opps.loc[opps["ex"].idxmin()]
                            post_level_ex = row_ple.to_dict()

                # проверка EMA vs post_level_ex
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

                # Outlier — для down
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

                # фиксируем Range
                seq_range_id += 1
                range_id_local = seq_range_id
                def_ts = current_ts
                def_date = datetime.datetime.utcfromtimestamp(current_ts).strftime("%Y-%m-%d %H:%M:%S")

                # формировка pri JSON
                pri_obj = None
                try:
                    pri_obj = build_pri_obj(level_row, level_ex, outlier_ex if outlier_detected else None, current_ts, margin)
                    pri_created += 1
                except Exception as e:
                    print("Ошибка формирования pri:", e)

                # формируем sec 
                sec_obj = build_sec(conn, df_ext, direction, level_ex, level_row, post_level_ex, margin, l2b_cond_mult, prl2min_diff, mwp)

                # Если sec найден, выполнить проверку mwp (min_width) и, при необходимости, пометить narrow
                if sec_obj is not None:
                    try:
                        if direction == "up":
                            top_border_value = pri_obj["level_value"] if pri_obj else None
                        else:
                            top_border_value = sec_obj["level_value"]
                        if top_border_value and top_border_value != 0 and pri_obj:
                            mwp_val = abs(pri_obj["level_value"] - sec_obj["level_value"]) * 100.0 / abs(top_border_value)
                            if mwp_val < mwp:
                                # narrow situation: оставляем sec, но ставим флаг narrow=True
                                sec_obj["narrow"] = True
                                # попытка подмены на last borders сделана внутри build_sec partially;
                        # Убедимся, что sec_obj имеет top/bottom по margin
                        sec_obj["top"] = sec_obj["level_value"] * (1 + margin / 100.0)
                        sec_obj["bottom"] = sec_obj["level_value"] * (1 - margin / 100.0)
                    except Exception:
                        pass

                # sec ставим как JSON-объект (или None)
                sec_info = sec_obj

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

                # завершить текущий уровень
                try:
                    finalize_current_level(conn, int(level_row.get("id")) if level_row.get("id") is not None else None, current_ts)
                except Exception as e:
                    print("Ошибка при finalize:", e)

                # сброс состояния
                range_start_point = None
                range_start_idx = None
                range_cancelled = False

    print(f"Добавлено записей в bf_range: {ranges_created}, сформировано pri JSON: {pri_created}")

    # вывести пример одной добавленной записи (последняя)
    if ranges_created > 0:
        with pg_conn.cursor() as cur:
            cur.execute("SELECT id, range_id, pri, sec FROM bf_range ORDER BY id DESC LIMIT 1")
            r = cur.fetchone()
            if r:
                print("Пример последней записи (pri, sec):")
                print("id:", r[0], "range_id:", r[1])
                print("pri:", r[2])
                print("sec:", r[3])


# Запуск

if __name__ == "__main__":
    try:
        create_range_table(pg_conn)
        detect_range(pg_conn)
    except Exception as e:
        import traceback
        print("Ошибка выполнения:")
        traceback.print_exc()
