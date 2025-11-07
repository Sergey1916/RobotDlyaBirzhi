import psycopg2
import pandas as pd
import datetime
import math
import json
import sys
from typing import Optional, List, Dict, Any

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


def finalize_level_by_id(conn, level_db_id: Optional[int], ts: int, reason: str = "cancel"):
    if level_db_id is None:
        return
    end_date = datetime.datetime.fromtimestamp(int(ts)).strftime("%Y-%m-%d %H:%M:%S")
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE bf_levels
            SET end_ts = %s,
                end_date = %s,
                complete_reason = %s,
                status = 'inactive'
            WHERE id = %s
        """, (ts, end_date, reason, level_db_id))
    conn.commit()


def build_pri_obj(current_level_row: dict, level_ex: dict, outlier_ex: Optional[dict], ts: int, margin: float) -> Dict[str, Any]:
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


def candidate_from_ext(ex_row: dict, margin: float, source_label: str) -> Dict[str, Any]:
    val = float(ex_row["ex"])
    return {
        "source": source_label,
        "level_value": val,
        "top": val * (1 + margin / 100.0),
        "bottom": val * (1 - margin / 100.0),
        "ex_start": int(ex_row.get("id") or 0),
        "ex_start_date": datetime.datetime.fromtimestamp(int(ex_row["timestamp"])).strftime("%m/%d/%Y %H:%M:%S")
    }


# pre_level_ex (пункт 3)

def find_pre_level_ex(df_ext: pd.DataFrame, level_ex: dict, post_level_ex: Optional[dict], direction: str) -> Optional[dict]:
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



def build_sec_full(
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
    pri_obj: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    secondary_levels: List[Dict[str, Any]] = []

    # 1) post_level_ex 
    if post_level_ex is not None:
        secondary_levels.append(candidate_from_ext(post_level_ex, margin, "post_level_ex"))

    # 2) last_range
    last_range = get_last_range_from_db(conn)
    if last_range is not None:
        try:
            last_dir = (last_range.get("direction") or "").lower()
            last_border = None
            if direction == "up":
                # last_border 
                if last_dir == "up":
                    last_border = json.loads(last_range.get("pri") or "null")
                    if isinstance(last_border, list):
                        last_border = last_border[0]
                else:
                    last_border = json.loads(last_range.get("sec") or "null")
            else:
                # down: last_border = secondary previous если prev был up, или primary если down
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
                        appended = {
                            "source": "last_border",
                            "level_value": last_val,
                            "top": last_val * (1 + margin / 100.0),
                            "bottom": last_val * (1 - margin / 100.0),
                            "ex_start": int(last_border.get("ex_start", 0)),
                            "ex_start_date": last_border.get("ex_start_date", "")
                        }
                        try:
                            prev_start = int(last_range.get("start_ts") or 0)
                            prev_end = int(last_range.get("end_ts") or 0)
                            if prev_start and prev_end and prev_end > prev_start:
                                q = """
                                    SELECT timestamp, type, ex
                                    FROM bf_sr2_8
                                    WHERE timestamp BETWEEN %s AND %s
                                    ORDER BY timestamp ASC
                                """
                                df_prev_ext = pd.read_sql_query(q, pg_conn, params=(prev_start, prev_end))
                                if not df_prev_ext.empty:
                                    # ищем last minimum в prev_range
                                    prev_mins = df_prev_ext[df_prev_ext["type"] == "min"]
                                    if not prev_mins.empty:
                                        last_min_row = prev_mins.iloc[-1]
                                        last_min_val = float(last_min_row["ex"])
                                        if last_min_val < last_val * (1 + prl2min_diff / 100.0):
                                            appended = {
                                                "source": "last_min_prev_range",
                                                "level_value": last_min_val,
                                                "top": last_min_val * (1 + margin / 100.0),
                                                "bottom": last_min_val * (1 - margin / 100.0),
                                                "ex_start": int(last_min_row.get("id") or 0),
                                                "ex_start_date": datetime.datetime.fromtimestamp(int(last_min_row["timestamp"])).strftime("%m/%d/%Y %H:%M:%S")
                                            }
                        except Exception:
                            pass
                        secondary_levels.append(appended)
        except Exception:
            pass

    # 3) pre_level_ex
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

    # берем last
    sec_n = secondary_levels[-1].copy()
    sec_n["status"] = "active"
    sec_n["start_ts"] = int(level_ex["timestamp"])
    sec_n["start_date"] = datetime.datetime.fromtimestamp(int(level_ex["timestamp"])).strftime("%m/%d/%Y %H:%M:%S")
    sec_n["create_reason"] = "range"
    sec_n["rules"] = ["shift", "cross_turn"]
    sec_n["type"] = "resistance" if direction == "up" else "support"
    sec_n["kind"] = "main"

    sec_n["top"] = sec_n.get("top", sec_n["level_value"] * (1 + margin / 100.0))
    sec_n["bottom"] = sec_n.get("bottom", sec_n["level_value"] * (1 - margin / 100.0))

    # mwp  -  top_border_value (pri для up, sec для down)
    try:
        if direction == "up":
            top_border_value = pri_obj["level_value"]
        else:
            top_border_value = sec_n["level_value"]
        if top_border_value and top_border_value != 0:
            mwp_val = abs(pri_obj["level_value"] - sec_n["level_value"]) * 100.0 / abs(top_border_value)
            if mwp_val < mwp:
                sec_n["narrow"] = True
            else:
                sec_n["narrow"] = False
        else:
            sec_n["narrow"] = False
    except Exception:
        sec_n["narrow"] = False

    return sec_n


def insert_range_db(conn, data: dict) -> int:
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
            RETURNING id
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
        rid = cur.fetchone()[0]
    conn.commit()
    return int(rid)


def close_range_db(conn, db_id: int, ts: int):
    end_date = datetime.datetime.fromtimestamp(int(ts)).strftime("%Y-%m-%d %H:%M:%S")
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE bf_range
            SET end_ts = %s,
                end_date = %s
            WHERE id = %s
        """, (ts, end_date, db_id))
    conn.commit()


def get_last_range_from_db(conn) -> Optional[dict]:
    with conn.cursor() as cur:
        cur.execute("SELECT id, range_id, direction, pri, sec, start_ts, end_ts FROM bf_range ORDER BY id DESC LIMIT 1")
        row = cur.fetchone()
        if not row:
            return None
        return {
            "id": row[0],
            "range_id": row[1],
            "direction": row[2],
            "pri": row[3],
            "sec": row[4],
            "start_ts": row[5],
            "end_ts": row[6]
        }


def detect_range(conn):
    print("Загружаем параметры и данные.")
    params = fetch_params(conn)

    margin = float(params.get("margin", 0))
    define_hours = float(params.get("define_hours", 1))
    ema_length = int(params.get("ema_length", 20) if params.get("ema_length") is not None else 20)
    l2b_cond_mult = float(params.get("l2b_cond_mult", 1))
    prl2min_diff = float(params.get("prl2min_diff", 0))
    mwp = float(params.get("mwp", 0))
    outlier_enabled = bool(params.get("Outlier", params.get("outlier", False)))
    outlier_min_diff = float(params.get("outlier_min_diff", 0))
    outlier_max_hours = float(params.get("outlier_max_hours", 0))

    df_levels = fetch_levels(conn, include_all=True)
    df_ext = fetch_extremes(conn)
    if df_ext.empty:
        print("Нет экстремумов (active) в bf_sr2_8 — выходим.")
        return

    df_ext = df_ext.sort_values("timestamp").reset_index(drop=True)
    df_ext["ema"] = df_ext["ex"].ewm(span=ema_length, adjust=False).mean()

    ext_by_id = {int(r["id"]): r.to_dict() for _, r in df_ext.iterrows() if "id" in r}

    range_start_point = None
    range_start_idx = None
    range_cancelled = False

    seq_range_id = 0
    ranges_created = 0
    pri_created = 0

    active_range: Optional[Dict[str, Any]] = None

    print("Старт обхода экстремумов...")

    for idx, row in df_ext.iterrows():
        current_ts = int(row["timestamp"])
        ema = float(row["ema"])
        direction = str(row["direction"]).lower()
        ex_val = float(row["ex"])

        if active_range is not None:
            try:
                pri = active_range.get("pri_obj")
                sec = active_range.get("sec_obj")
                pri_top = pri["top"]
                pri_bottom = pri["bottom"]
                sec_top = sec["top"] if sec else None
                sec_bottom = sec["bottom"] if sec else None

                # cross_new_range:
                if active_range["direction"] == "up":
                    # up: если EMA > outer upper primary -> cross_new_range
                    if ema > pri_top:
                        # close current range
                        close_range_db(conn, active_range["db_id"], current_ts)
                        try:
                            finalize_level_by_id(conn, pri.get("level_id"), current_ts, reason="cancel")
                            if sec and sec.get("level_id"):
                                finalize_level_by_id(conn, sec.get("level_id"), current_ts, reason="cancel")
                        except Exception:
                            pass
                        print(f"cross_new_range (up) at {current_ts}, closed range id {active_range['db_id']}")
                        active_range = None
                        range_start_point = current_ts
                        range_start_idx = idx
                        range_cancelled = False
                        continue
                else:  # active direction down
                    if ema < pri_bottom:
                        close_range_db(conn, active_range["db_id"], current_ts)
                        try:
                            finalize_level_by_id(conn, pri.get("level_id"), current_ts, reason="cancel")
                            if sec and sec.get("level_id"):
                                finalize_level_by_id(conn, sec.get("level_id"), current_ts, reason="cancel")
                        except Exception:
                            pass
                        print(f"cross_new_range (down) at {current_ts}, closed range id {active_range['db_id']}")
                        active_range = None
                        range_start_point = current_ts
                        range_start_idx = idx
                        range_cancelled = False
                        continue

                # cross_turn
                if active_range["direction"] == "up":
                    # для up: если EMA < external lower boundary (sec.bottom) -> cross_turn
                    if sec is not None and sec_bottom is not None and ema < sec_bottom:
                        # закрываем current range
                        close_range_db(conn, active_range["db_id"], current_ts)
                        try:
                            finalize_level_by_id(conn, pri.get("level_id"), current_ts, reason="cancel")
                            if sec and sec.get("level_id"):
                                finalize_level_by_id(conn, sec.get("level_id"), current_ts, reason="cancel")
                        except Exception:
                            pass
                        print(f"cross_turn (up->down) at {current_ts}, closed range id {active_range['db_id']}")
                        # меняем direction
                        direction = "down"
                        active_range = None

                        range_start_point = current_ts
                        range_start_idx = idx
                        range_cancelled = False
                        continue
                else:
                    # для down: если EMA > external upper boundary (sec.top) -> cross_turn
                    if sec is not None and sec_top is not None and ema > sec_top:
                        close_range_db(conn, active_range["db_id"], current_ts)
                        try:
                            finalize_level_by_id(conn, pri.get("level_id"), current_ts, reason="cancel")
                            if sec and sec.get("level_id"):
                                finalize_level_by_id(conn, sec.get("level_id"), current_ts, reason="cancel")
                        except Exception:
                            pass
                        print(f"cross_turn (down->up) at {current_ts}, closed range id {active_range['db_id']}")
                        direction = "up"
                        active_range = None
                        range_start_point = current_ts
                        range_start_idx = idx
                        range_cancelled = False
                        continue
            except Exception as e:
                print("Ошибка при проверке cross правил:", e)


        if direction == "down":
            subset = df_levels[df_levels["type"].astype(str).str.lower() == "support"]
        else:
            subset = df_levels[df_levels["type"].astype(str).str.lower() == "resistance"]

        if subset.empty:
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

        # границы
        try:
            cl_top = float(level_row.get("top")) if level_row.get("top") is not None else float("nan")
        except:
            cl_top = float("nan")
        try:
            cl_bottom = float(level_row.get("bottom")) if level_row.get("bottom") is not None else float("nan")
        except:
            cl_bottom = float("nan")

        crossed = False
        if direction == "down":
            if not math.isnan(cl_bottom) and ema < cl_bottom:
                crossed = True
        else:
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
                # подготовка level_ex
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

                # post_level_ex поиск
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

                # Outlier (down)
                outlier_detected = False
                outlier_ex = None
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
                        except Exception:
                            outlier_detected = False

                # создаем range
                seq_range_id += 1
                def_ts = current_ts
                def_date = datetime.datetime.utcfromtimestamp(current_ts).strftime("%Y-%m-%d %H:%M:%S")

                try:
                    pri_obj = build_pri_obj(level_row, level_ex, outlier_ex if outlier_detected else None, current_ts, margin)
                    pri_created += 1
                except Exception as e:
                    print("Ошибка формирования pri:", e)
                    pri_obj = None

                # создаем sec
                sec_obj = build_sec_full(conn, df_ext, direction, level_ex, level_row, post_level_ex,
                                         margin, l2b_cond_mult, prl2min_diff, mwp, pri_obj)

                data = {
                    "range_id": seq_range_id,
                    "direction": direction,
                    "def_ts": def_ts,
                    "def_date": def_date,
                    "start_ts": range_start_point,
                    "start_date": datetime.datetime.utcfromtimestamp(range_start_point).strftime("%Y-%m-%d %H:%M:%S"),
                    "le_date": def_date,
                    "end_ts": None,
                    "end_date": None,
                    "pri": [pri_obj] if pri_obj is not None else None,
                    "sec": sec_obj,
                    "create_reason": "NEW"
                }

                # вставляем в бд
                try:
                    db_id = insert_range_db(conn, data)
                    print(f"Создан range id_db={db_id}, range_id_seq={seq_range_id}, ts={def_ts}")
                    active_range = {
                        "db_id": db_id,
                        "range_id": seq_range_id,
                        "direction": direction,
                        "pri_obj": pri_obj,
                        "sec_obj": sec_obj
                    }
                except Exception as e:
                    print("Ошибка вставки range в БД:", e)
                    active_range = None

                try:
                    finalize_level_by_id(conn, int(level_row.get("id")) if level_row.get("id") is not None else None, current_ts, reason="activation")
                except Exception:
                    pass

                ranges_created += 1
                range_start_point = None
                range_start_idx = None
                range_cancelled = False

    print(f"Добавлено записей в bf_range: {ranges_created}, сформировано pri JSON: {pri_created}")


    if ranges_created > 0:
        with conn.cursor() as cur:
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
    except Exception:
        import traceback
        traceback.print_exc()
        sys.exit(1)
