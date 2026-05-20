# v1.0.0 — StarRocks version of webhook_monitoring_timeout-001.py
# Replaces Elasticsearch queries with StarRocks SQL (CASE WHEN replaces runtime mappings)
import pandas as pd
import numpy as np
import warnings
import requests
import json
import os
import datetime as date
import pytz
import time
import pymysql
warnings.filterwarnings('ignore')

# ── StarRocks connection ──────────────────────────────────────────────

STARROCKS_CONFIG = {
    "host": os.environ.get("STARROCKS_HOST", "127.0.0.1"),
    "port": int(os.environ.get("STARROCKS_PORT", 9030)),
    "user": os.environ.get("STARROCKS_USER", "root"),
    "password": os.environ.get("STARROCKS_PASSWORD", ""),
    "database": os.environ.get("STARROCKS_DATABASE", "bronze"),
    "charset": "utf8mb4",
}

# Table names (adjust to match your Polaris/StarRocks catalog)
TABLE_TRANSACTION = os.environ.get("STARROCKS_TRX_TABLE", "bronze.fms_raw.logstash_transaction")


def connect_starrocks():
    return pymysql.connect(**STARROCKS_CONFIG)


def query_to_df(sql, params=None):
    conn = connect_starrocks()
    try:
        df = pd.read_sql(sql, conn, params=params)
    finally:
        conn.close()
    return df


# ── trx_status CASE WHEN (replaces ES runtime mapping) ────────────────
# The original ES script classifies each row via a Painless runtime script.
# In StarRocks we express the same logic as a SQL CASE expression.

TRX_STATUS_EXPR = """
    CASE
        WHEN rc IN ('Q1','Q2','Q3','Q4')
            THEN 'bri_issue'
        WHEN rd IN (
                'TC - Reversal Timeout',
                'TC - Timeout (transactionReversalReq : success)',
                'TC - Timeout (transactionReversalReq : failed)')
            THEN 'reversal_timeout'
        WHEN rd LIKE '%Timeout%' OR rd LIKE '%Time Out%'
            THEN 'timeout'
        WHEN (rd IN ('APPROVED','APPROVED - failed to update')) AND rc = '00'
            THEN 'success'
        WHEN rd LIKE '%Q1%' OR rd LIKE '%Q2%' OR rd LIKE '%Q3%' OR rd LIKE '%Q4%'
            THEN 'bri_issue'
        ELSE 'other'
    END
"""


# ── Helper classes & functions (unchanged from original) ──────────────

class CurrDate:
    def __init__(self):
        self.curr_date = date.datetime.today() + date.timedelta(hours=7)

    def year_str(self): return str(self.curr_date.year)
    def month_str(self):
        m = str(self.curr_date.month)
        return m.zfill(2)
    def day_str(self):
        d = str(self.curr_date.day)
        return d.zfill(2)
    def hour_str(self):
        h = str(self.curr_date.hour)
        return h.zfill(2)


def send_webhook_notification(webhook_url, card):
    headers = {'Content-Type': 'application/json; charset=UTF-8'}
    payload = {"cards": [card]}
    response = requests.post(webhook_url, data=json.dumps(payload), headers=headers)
    if response.status_code == 200:
        print("Notification sent successfully!")
    else:
        print(f"Failed to send notification. Status code: {response.status_code}, Response: {response.text}")


def card_summary(first_time, last_time, duration, timeout_unique_tid, success_timeout_tid, bri_issue_tid, timeout_feature, bri_feature):
    return _build_card("✅ Transaction Timeout Resolved", first_time, last_time, duration,
                       timeout_unique_tid, success_timeout_tid, bri_issue_tid,
                       timeout_feature, bri_feature)


def card_event(first_time, last_time, duration, timeout_unique_tid, success_timeout_tid, bri_issue_tid, timeout_feature, bri_feature):
    return _build_card("🚨 Transaction Timeout Alert 🚨", first_time, last_time, duration,
                       timeout_unique_tid, success_timeout_tid, bri_issue_tid,
                       timeout_feature, bri_feature)


def _build_card(title, first_time, last_time, duration, timeout_unique_tid, success_timeout_tid, bri_issue_tid, timeout_feature, bri_feature):
    return {
        "header": {"title": title},
        "sections": [
            {
                "widgets": [
                    {"keyValue": {"topLabel": "Total Unique Timeout TID", "content": str(timeout_unique_tid)}},
                    {"keyValue": {"topLabel": "Total Unique Success TID", "content": str(success_timeout_tid)}},
                    {"textParagraph": {"text": "Top Timeout Features:\n" + str(timeout_feature)}},
                ]
            },
            {
                "widgets": [
                    {"keyValue": {"topLabel": "Total unique TID on BRI Issue (Q1, Q2, Q3, Q4)", "content": str(bri_issue_tid)}},
                    {"textParagraph": {"text": "Top BRI Features:\n" + str(bri_feature)}},
                ]
            },
            {
                "widgets": [
                    {"keyValue": {"topLabel": "Duration", "content": f"{str(duration)} minute"}},
                    {"keyValue": {"topLabel": "Timeout Time", "content": f"{str(first_time)} to {str(last_time)}"}},
                ]
            },
        ]
    }


def format_top_feature_percent(feature_dict, total):
    if not isinstance(feature_dict, dict) or total == 0:
        return ''
    return '\n'.join([f"- {k}: {round((v / total) * 100)}%" for k, v in feature_dict.items()])


# ── StarRocks query functions (replace ES aggregations) ───────────────

def get_unique_tid(start, end):
    """
    Equivalent of the original ES query: classify transactions by status,
    then return unique TID counts and top features per status bucket
    for a given time range.

    Args:
        start: UTC datetime string 'YYYY-MM-DDTHH:MM:SS.000Z'
        end:   UTC datetime string 'YYYY-MM-DDTHH:MM:SS.000Z'
    """
    # Per-status unique TID counts
    sql_counts = f"""
        SELECT
            {TRX_STATUS_EXPR} AS trx_status,
            COUNT(DISTINCT acq_tid) AS unique_tid,
            COUNT(*) AS total_trx
        FROM {TABLE_TRANSACTION}
        WHERE created_at >= %s
          AND created_at <= %s
          AND {TRX_STATUS_EXPR} IN ('timeout', 'success', 'bri_issue')
        GROUP BY trx_status
    """
    df_counts = query_to_df(sql_counts, params=[start, end])

    # Top 5 payment_features for timeout
    sql_top_timeout = f"""
        SELECT payment_features, COUNT(*) AS cnt
        FROM {TABLE_TRANSACTION}
        WHERE created_at >= %s AND created_at <= %s
          AND {TRX_STATUS_EXPR} = 'timeout'
        GROUP BY payment_features
        ORDER BY cnt DESC
        LIMIT 5
    """
    df_top_timeout = query_to_df(sql_top_timeout, params=[start, end])

    # Top 5 payment_features for bri_issue
    sql_top_bri = f"""
        SELECT payment_features, COUNT(*) AS cnt
        FROM {TABLE_TRANSACTION}
        WHERE created_at >= %s AND created_at <= %s
          AND {TRX_STATUS_EXPR} = 'bri_issue'
        GROUP BY payment_features
        ORDER BY cnt DESC
        LIMIT 5
    """
    df_top_bri = query_to_df(sql_top_bri, params=[start, end])

    # Build result matching the original DataFrame shape
    counts_map = {row['trx_status']: row for _, row in df_counts.iterrows()} if not df_counts.empty else {}

    timeout_count = counts_map.get('timeout', {}).get('total_trx', 0) if counts_map else 0
    bri_count = counts_map.get('bri_issue', {}).get('total_trx', 0) if counts_map else 0

    timeout_feature_map = dict(zip(df_top_timeout['payment_features'], df_top_timeout['cnt'])) if not df_top_timeout.empty else {}
    bri_feature_map = dict(zip(df_top_bri['payment_features'], df_top_bri['cnt'])) if not df_top_bri.empty else {}

    row = {
        'success_trx': int(counts_map.get('success', {}).get('total_trx', 0)),
        'timeout_trx': int(timeout_count),
        'bri_issue_trx': int(bri_count),
        'success_unique_tid': int(counts_map.get('success', {}).get('unique_tid', 0)),
        'timeout_unique_tid': int(counts_map.get('timeout', {}).get('unique_tid', 0)),
        'bri_issue_tid': int(counts_map.get('bri_issue', {}).get('unique_tid', 0)),
        'timeout_top_feature': format_top_feature_percent(timeout_feature_map, timeout_count),
        'bri_top_feature': format_top_feature_percent(bri_feature_map, bri_count),
    }
    return pd.DataFrame([row])


def get_data_trx_timeout():
    """
    Equivalent of the original ES query: last 5 minutes, 1-minute buckets,
    classify per bucket and return per-minute aggregation DataFrame.
    """
    sql = f"""
        SELECT
            DATE_FORMAT(created_at, '%%Y-%%m-%%d %%H:%%i:00') AS created_at_min,
            {TRX_STATUS_EXPR} AS trx_status,
            COUNT(*) AS total_trx,
            COUNT(DISTINCT acq_tid) AS unique_tid
        FROM {TABLE_TRANSACTION}
        WHERE created_at >= NOW() - INTERVAL 5 MINUTE
        GROUP BY created_at_min, trx_status
        ORDER BY created_at_min ASC
    """
    df_raw = query_to_df(sql)

    if df_raw.empty:
        return pd.DataFrame()

    # Top 5 timeout features (last 5 min, global)
    sql_top_timeout = f"""
        SELECT payment_features, COUNT(*) AS cnt
        FROM {TABLE_TRANSACTION}
        WHERE created_at >= NOW() - INTERVAL 5 MINUTE
          AND {TRX_STATUS_EXPR} = 'timeout'
        GROUP BY payment_features
        ORDER BY cnt DESC
        LIMIT 5
    """
    df_top_timeout = query_to_df(sql_top_timeout)

    # Top 5 bri_issue features (last 5 min, global)
    sql_top_bri = f"""
        SELECT payment_features, COUNT(*) AS cnt
        FROM {TABLE_TRANSACTION}
        WHERE created_at >= NOW() - INTERVAL 5 MINUTE
          AND {TRX_STATUS_EXPR} = 'bri_issue'
        GROUP BY payment_features
        ORDER BY cnt DESC
        LIMIT 5
    """
    df_top_bri = query_to_df(sql_top_bri)

    timeout_feature_map = dict(zip(df_top_timeout['payment_features'], df_top_timeout['cnt'])) if not df_top_timeout.empty else {}
    bri_feature_map = dict(zip(df_top_bri['payment_features'], df_top_bri['cnt'])) if not df_top_bri.empty else {}

    # Pivot into per-minute rows (matching original DataFrame shape)
    rows = []
    for created_at_min, group in df_raw.groupby('created_at_min'):
        status_map = {row['trx_status']: row for _, row in group.iterrows()}

        timeout_trx = int(status_map.get('timeout', {}).get('total_trx', 0))
        bri_issue_trx = int(status_map.get('bri_issue', {}).get('total_trx', 0))
        timeout_unique_tid = int(status_map.get('timeout', {}).get('unique_tid', 0))
        success_unique_tid = int(status_map.get('success', {}).get('unique_tid', 0))
        bri_issue_tid = int(status_map.get('bri_issue', {}).get('unique_tid', 0))

        rows.append({
            'created_at': created_at_min,
            'success_trx': int(status_map.get('success', {}).get('total_trx', 0)),
            'timeout_trx': timeout_trx,
            'bri_issue_trx': bri_issue_trx,
            'success_unique_tid': success_unique_tid,
            'timeout_unique_tid': timeout_unique_tid,
            'bri_issue_tid': bri_issue_tid,
            'timeout_top_feature': format_top_feature_percent(timeout_feature_map, timeout_trx),
            'bri_top_feature': format_top_feature_percent(bri_feature_map, bri_issue_trx),
        })

    return pd.DataFrame(rows)


# ── State management & CSV helpers (unchanged from original) ──────────

def upsert_transaction_data(new_df, csv_path="/project/data/timeout/temp_trx_summary.csv"):
    if os.path.exists(csv_path):
        existing_df = pd.read_csv(csv_path, parse_dates=["created_at"])
    else:
        existing_df = pd.DataFrame(columns=["created_at", "success_trx", "timeout_trx"])

    new_df["created_at"] = pd.to_datetime(new_df["created_at"])
    existing_df["created_at"] = pd.to_datetime(existing_df["created_at"])
    existing_df.set_index("created_at", inplace=True)
    new_df.set_index("created_at", inplace=True)
    existing_df.update(new_df)
    combined_df = pd.concat([existing_df, new_df[~new_df.index.isin(existing_df.index)]])
    combined_df = combined_df.sort_index()
    combined_df.reset_index().to_csv(csv_path, index=False)
    print(f"✅ Upsert complete. File updated: {csv_path}")


def upsert_temp_data(
    range_time_curr=None,
    timeout_unique_tid=None,
    success_unique_tid=None,
    bri_issue_tid=None,
    timeout_feature=None,
    bri_feature=None,
    csv_path="/project/data/timeout/temp_sum.csv",
    clear=False
):
    if clear:
        empty_df = pd.DataFrame(columns=['range_time_curr', 'timeout_unique_tid', 'success_unique_tid', 'bri_issue_tid', 'timeout_feature', 'bri_feature'])
        empty_df.to_csv(csv_path, index=False)
        return empty_df

    new_df = pd.DataFrame({
        'range_time_curr': [range_time_curr],
        'timeout_unique_tid': [timeout_unique_tid],
        'success_unique_tid': [success_unique_tid],
        'bri_issue_tid': [bri_issue_tid],
        'timeout_top_feature': [timeout_feature],
        'bri_top_feature': [bri_feature]
    })

    if not os.path.exists(csv_path) or pd.read_csv(csv_path).empty:
        new_df.to_csv(csv_path, index=False)
        return new_df
    else:
        new_df.to_csv(csv_path, index=False)
        return new_df


def flush_temp_to_history(first_time, last_time, temp_path="/project/data/timeout/temp_trx_summary.csv", history_path="/project/data/timeout/historical_trx_summary.csv"):
    if not os.path.exists(temp_path):
        print("🚫 Temp file doesn't exist.")
        return

    temp_df = pd.read_csv(temp_path, parse_dates=["created_at"])
    temp_df = temp_df[(temp_df['created_at'] >= first_time) & (temp_df['created_at'] <= last_time)]

    if temp_df.empty:
        print("⚠️ Temp file is empty, nothing to move.")
        return

    if os.path.exists(history_path):
        history_df = pd.read_csv(history_path, parse_dates=["created_at"])
    else:
        history_df = pd.DataFrame(columns=["created_at", "success_trx", "timeout_trx"])

    history_df.set_index("created_at", inplace=True)
    temp_df.set_index("created_at", inplace=True)
    history_df.update(temp_df)
    combined_df = pd.concat([history_df, temp_df[~temp_df.index.isin(history_df.index)]])
    combined_df = combined_df.sort_index()
    combined_df.reset_index().to_csv(history_path, index=False)

    temp_df = pd.DataFrame(columns=["created_at", "success_trx", "timeout_trx"])
    temp_df.to_csv(temp_path, index=False)
    print("✅ Temp data moved to historical & temp cleared.")


def set_default():
    with open("/project/data/timeout/timeout_time.json", "w") as file:
        json.dump({"first_timeout_time": None, "first_timeout_status": 0, "latest_timeout_time": None, "range_time": "-"}, file)


def set_variable_timeout(fto, ftos, lto, range_time):
    with open("/project/data/timeout/timeout_time.json", "w") as file:
        json.dump({"first_timeout_time": fto, "first_timeout_status": ftos, "latest_timeout_time": lto, "range_time": range_time}, file)


def load_timeout_time():
    if os.path.exists("/project/data/timeout/timeout_time.json"):
        with open("/project/data/timeout/timeout_time.json", "r") as file:
            data = json.load(file)
            return (data.get("first_timeout_time"), data.get("first_timeout_status"),
                    data.get("latest_timeout_time"), data.get("range_time"))


def convert_utc_to_jakarta(time_str):
    return pd.to_datetime(time_str).tz_convert("Asia/Jakarta").strftime("%Y-%m-%d %H:%M:%S")


def convert_jakarta_to_utc(time_str):
    local_tz = pytz.timezone('Asia/Jakarta')
    local_dt = local_tz.localize(date.datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S"))
    return local_dt.astimezone(pytz.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


# ── Main processing logic (unchanged from original) ──────────────────

def trx_timeout_process():
    data = get_data_trx_timeout()
    url = 'https://chat.googleapis.com/v1/spaces/AAQAlaHBu1Q/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=Hz4o76kwv4j1y4YWWZcE6JUC7Nah5utumE9Dj0fgxY4'

    if not data.empty:
        latest_row = data.iloc[-1]
        if (latest_row['success_trx'] + latest_row['timeout_trx']) >= 0:
            first_time, first_timeout_status, last_time, range_time = load_timeout_time()

            if first_timeout_status == 0:
                if not data[(data['timeout_unique_tid'] > 30) | (data['bri_issue_tid'] > 30)].empty:
                    filtered_df = data[(data['timeout_unique_tid'] > 30) | (data['bri_issue_tid'] > 30)]
                    data['created_at'] = pd.to_datetime(data['created_at'])

                    first_to = convert_utc_to_jakarta(filtered_df['created_at'].min())
                    last_to_max = convert_utc_to_jakarta(filtered_df['created_at'].max())
                    last_to_dt = date.datetime.strptime(last_to_max, "%Y-%m-%d %H:%M:%S")
                    last_to = last_to_dt.replace(second=59).strftime("%Y-%m-%d %H:%M:%S")

                    first_to_dt = pd.to_datetime(first_to).tz_localize('Asia/Jakarta')
                    last_to_dt = pd.to_datetime(last_to).tz_localize('Asia/Jakarta')

                    range_time_curr = f"{first_to} to {last_to}"
                    data = data[(data['created_at'] >= first_to_dt) & (data['created_at'] <= last_to_dt)]
                    timeout_unique_tid = data['timeout_unique_tid'].sum()
                    success_unique_tid = data['success_unique_tid'].sum()
                    bri_issue_tid = data['bri_issue_tid'].sum()
                    timeout_feature = data['timeout_top_feature'].iloc[-1]
                    bri_feature = data['bri_top_feature'].iloc[-1]

                    upsert_transaction_data(data)
                    card = card_event(first_to, last_to, 1, timeout_unique_tid, success_unique_tid, bri_issue_tid, timeout_feature, bri_feature)
                    send_webhook_notification(url, card)
                    set_variable_timeout(first_to, 1, last_to, range_time_curr)
                    upsert_temp_data(range_time_curr, timeout_unique_tid, success_unique_tid, bri_issue_tid, timeout_feature, bri_feature)
                else:
                    print("no timeout")
            else:
                if not data[(data['timeout_unique_tid'] > 30) | (data['bri_issue_tid'] > 30)].empty:
                    first_to = first_time
                    filtered_df = data[(data['timeout_unique_tid'] > 30) | (data['bri_issue_tid'] > 30)]
                    last_to_max = convert_utc_to_jakarta(filtered_df['created_at'].max())
                    last_to_dt = date.datetime.strptime(last_to_max, "%Y-%m-%d %H:%M:%S")
                    last_to = last_to_dt.replace(second=59).strftime("%Y-%m-%d %H:%M:%S")

                    range_time_curr = f"{first_to} to {last_to}"
                    prev_range_time = range_time

                    upsert_transaction_data(data)

                    data_sum = pd.read_csv("/project/data/timeout/temp_trx_summary.csv")
                    filtered_data_sum = data_sum[(data_sum['created_at'] >= first_to) & (data_sum['created_at'] <= last_to)]

                    start = convert_jakarta_to_utc(first_to)
                    end = convert_jakarta_to_utc(last_to)
                    data_tid = get_unique_tid(start, end)

                    timeout_unique_tid = int(data_tid['timeout_unique_tid'].sum()) if 'timeout_unique_tid' in data_tid.columns else 0
                    success_unique_tid = int(data_tid['success_unique_tid'].sum()) if 'success_unique_tid' in data_tid.columns else 0
                    bri_issue_tid = int(data_tid['bri_issue_tid'].sum()) if 'bri_issue_tid' in data_tid.columns else 0
                    timeout_feature = data_tid['timeout_top_feature'].iloc[-1] if 'timeout_top_feature' in data_tid.columns and not data_tid.empty else "-"
                    bri_feature = data_tid['bri_top_feature'].iloc[-1] if 'bri_top_feature' in data_tid.columns and not data_tid.empty else "-"

                    if range_time_curr != prev_range_time:
                        duration = date.datetime.strptime(last_to, "%Y-%m-%d %H:%M:%S") - date.datetime.strptime(first_to, "%Y-%m-%d %H:%M:%S")
                        duration = int(duration.total_seconds() / 60) + 1
                        card = card_event(first_to, last_to, duration, timeout_unique_tid, success_unique_tid, bri_issue_tid, timeout_feature, bri_feature)
                        send_webhook_notification(url, card)
                        set_variable_timeout(first_to, 1, last_to, range_time_curr)
                        upsert_temp_data(range_time_curr, timeout_unique_tid, success_unique_tid, bri_issue_tid, timeout_feature, bri_feature)
                    else:
                        print("do nothing")
                else:
                    first_time, first_timeout_status, last_time, range_time = load_timeout_time()
                    data_sum = pd.read_csv("/project/data/timeout/temp_trx_summary.csv")
                    filtered_data_sum = data_sum[(data_sum['created_at'] >= first_time) & (data_sum['created_at'] <= last_time)]

                    first_to = date.datetime.strptime(first_time, "%Y-%m-%d %H:%M:%S")
                    last_to = date.datetime.strptime(last_time, "%Y-%m-%d %H:%M:%S")
                    duration = int((last_to - first_to).total_seconds() / 60) + 1

                    start = convert_jakarta_to_utc(first_time)
                    end = convert_jakarta_to_utc(last_time)
                    data_tid = get_unique_tid(start, end)

                    timeout_unique_tid = int(data_tid['timeout_unique_tid'].sum()) if 'timeout_unique_tid' in data_tid.columns else 0
                    success_unique_tid = int(data_tid['success_unique_tid'].sum()) if 'success_unique_tid' in data_tid.columns else 0
                    bri_issue_tid = int(data_tid['bri_issue_tid'].sum()) if 'bri_issue_tid' in data_tid.columns else 0
                    timeout_feature = data_tid['timeout_top_feature'].iloc[-1] if 'timeout_top_feature' in data_tid.columns and not data_tid.empty else "-"
                    bri_feature = data_tid['bri_top_feature'].iloc[-1] if 'bri_top_feature' in data_tid.columns and not data_tid.empty else "-"

                    latest_temp = pd.read_csv("/project/data/timeout/temp_sum.csv")
                    latest_timeout_tid = int(latest_temp.loc[0, 'timeout_unique_tid']) if 'timeout_unique_tid' in latest_temp.columns and not latest_temp.empty else 0
                    latest_success_tid = int(latest_temp.loc[0, 'success_unique_tid']) if 'success_unique_tid' in latest_temp.columns and not latest_temp.empty else 0
                    latest_bri_tid = int(latest_temp.loc[0, 'bri_issue_tid']) if 'bri_issue_tid' in latest_temp.columns and not latest_temp.empty else 0
                    latest_timeout_feature = latest_temp.loc[0, 'timeout_top_feature'] if 'timeout_top_feature' in latest_temp.columns and not latest_temp.empty else "-"
                    latest_bri_feature = latest_temp.loc[0, 'bri_top_feature'] if 'bri_top_feature' in latest_temp.columns and not latest_temp.empty else "-"

                    if timeout_unique_tid < latest_timeout_tid or success_unique_tid < latest_success_tid or bri_issue_tid < latest_bri_tid:
                        card = card_summary(first_to, last_to, duration, latest_timeout_tid, latest_success_tid, latest_bri_tid, latest_timeout_feature, latest_bri_feature)
                    else:
                        card = card_summary(first_to, last_to, duration, timeout_unique_tid, success_unique_tid, bri_issue_tid, timeout_feature, bri_feature)

                    send_webhook_notification(url, card)
                    flush_temp_to_history(first_time, last_time)
                    upsert_temp_data(upsert_temp_data(clear=True))
                    time.sleep(10)
                    set_default()
        else:
            print("Latest data has no transactions.")
    else:
        first_time, first_timeout_status, last_time, range_time = load_timeout_time()
        if first_timeout_status == 1:
            data_sum = pd.read_csv("/project/data/timeout/temp_trx_summary.csv")
            filtered_data_sum = data_sum[(data_sum['created_at'] >= first_time) & (data_sum['created_at'] <= last_time)]

            first_to_dt = date.datetime.strptime(first_time, "%Y-%m-%d %H:%M:%S")
            last_to_dt = date.datetime.strptime(last_time, "%Y-%m-%d %H:%M:%S")
            duration = int((last_to_dt - first_to_dt).total_seconds() / 60) + 1

            start = convert_jakarta_to_utc(first_time)
            end = convert_jakarta_to_utc(last_time)
            data_tid = get_unique_tid(start, end)

            timeout_unique_tid = int(data_tid['timeout_unique_tid'].sum()) if 'timeout_unique_tid' in data_tid.columns else 0
            success_unique_tid = int(data_tid['success_unique_tid'].sum()) if 'success_unique_tid' in data_tid.columns else 0
            bri_issue_tid = int(data_tid['bri_issue_tid'].sum()) if 'bri_issue_tid' in data_tid.columns else 0
            timeout_feature = data_tid['timeout_top_feature'].iloc[-1] if 'timeout_top_feature' in data_tid.columns and not data_tid.empty else "-"
            bri_feature = data_tid['bri_top_feature'].iloc[-1] if 'bri_top_feature' in data_tid.columns and not data_tid.empty else "-"

            latest_temp = pd.read_csv("/project/data/timeout/temp_sum.csv")
            latest_timeout_tid = int(latest_temp.loc[0, 'timeout_unique_tid']) if 'timeout_unique_tid' in latest_temp.columns and not latest_temp.empty else 0
            latest_success_tid = int(latest_temp.loc[0, 'success_unique_tid']) if 'success_unique_tid' in latest_temp.columns and not latest_temp.empty else 0
            latest_bri_tid = int(latest_temp.loc[0, 'bri_issue_tid']) if 'bri_issue_tid' in latest_temp.columns and not latest_temp.empty else 0
            latest_timeout_feature = latest_temp.loc[0, 'timeout_top_feature'] if 'timeout_top_feature' in latest_temp.columns and not latest_temp.empty else "-"
            latest_bri_feature = latest_temp.loc[0, 'bri_top_feature'] if 'bri_top_feature' in latest_temp.columns and not latest_temp.empty else "-"

            if timeout_unique_tid < latest_timeout_tid or success_unique_tid < latest_success_tid or bri_issue_tid < latest_bri_tid:
                card = card_summary(first_to_dt, last_to_dt, duration, latest_timeout_tid, latest_success_tid, latest_bri_tid, latest_timeout_feature, latest_bri_feature)
            else:
                card = card_summary(first_to_dt, last_to_dt, duration, timeout_unique_tid, success_unique_tid, bri_issue_tid, timeout_feature, bri_feature)

            send_webhook_notification(url, card)
            flush_temp_to_history(first_time, last_time)
            upsert_temp_data(upsert_temp_data(clear=True))
            set_default()
        else:
            print("no data")


trx_timeout_process()
