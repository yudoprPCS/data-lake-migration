# thermal_prediction_pipeline.py — Unified thermal paper prediction pipeline
# Merges: 1_head, 2.1_gqr, 2.2_stl, 2.3_trx, 3_adjusting, 4_to_elk
# Data source: StarRocks (replaces Elasticsearch)
# Idempotent: checkpoint-based resume per stage and per batch

from prophet import Prophet
import numpy as np
import pandas as pd
import datetime as date
import calendar
from dateutil.relativedelta import relativedelta
from hijri_converter import Hijri, Gregorian
import time
import json
import logging
import glob
import os
import shutil
import pymysql
from concurrent.futures import ProcessPoolExecutor, as_completed
import warnings
warnings.filterwarnings('ignore')

# ══════════════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════════════

STARROCKS_CONFIG = {
    "host": os.environ.get("STARROCKS_HOST", "127.0.0.1"),
    "port": int(os.environ.get("STARROCKS_PORT", 9030)),
    "user": os.environ.get("STARROCKS_USER", "root"),
    "password": os.environ.get("STARROCKS_PASSWORD", ""),
    "database": os.environ.get("STARROCKS_DATABASE", "bronze"),
    "charset": "utf8mb4",
}

# Table names (adjust to your Polaris/StarRocks catalog)
TBL_POPULATION = os.environ.get("TBL_POPULATION", "bronze.fms_raw.logstash_population_with_hb_trx_logon_ticket")
TBL_TRANSACTION = os.environ.get("TBL_TRANSACTION", "bronze.fms_raw.transform_transaction_summary_thermal_paper_consumption")
TBL_TICKET_MTC = os.environ.get("TBL_TICKET_MTC", "bronze.fms_raw.logstash_ticket_maintenance")
TBL_TICKET_IMPL = os.environ.get("TBL_TICKET_IMPL", "bronze.fms_raw.logstash_ticket_implementation")

BASE_DIR = os.environ.get("THERMAL_BASE_DIR", "/project/data/thermal_paper_prediction")

# Prediction params
SUM_CHOICE = list(range(50, 76))  # [50, 51, ..., 75]
PREDICT_X_DAYS = 70
BATCH_SIZE = 1000
MIN_DATA_DAYS = 90  # min days of data for Prophet; below → special_case prorate
ONE_ROLL_TOTAL_CM = 1150
CM_TRX_WITH_BRIMO = 14.7
CM_TRX_NO_BRIMO = 10
CM_SETTLEMENT = 36
CM_GENERATE_QR = 13.5
DEFAULT_PRINT_BEHAVIOR = 3
DEFAULT_REPRINT_BEHAVIOR = 0
DEFAULT_PM_NEEDS_ROLLS = 1
MAX_REPRINT_BEHAVIOR = 0.5

STAGES = ["reference_data", "predict_transaction", "predict_settlement", "predict_generate_qr", "transform", "adjust", "output"]

# ══════════════════════════════════════════════════════════════════════
# UTILITIES
# ══════════════════════════════════════════════════════════════════════

class CurrDate:
    def __init__(self):
        self.curr_date = date.datetime.today() + date.timedelta(hours=7)
    def year_int(self): return self.curr_date.year
    def month_int(self): return self.curr_date.month
    def day_int(self): return self.curr_date.day
    def to_ymd_str(self):
        return f"{self.curr_date.year}-{self.curr_date.month:02d}-{self.curr_date.day:02d}"


def log(msg):
    ts = (date.datetime.today() + date.timedelta(hours=7)).strftime("%Y-%m-%d %H:%M:%S.%f")
    print(f"[{ts}] {msg}")


def makedir(path):
    os.makedirs(path, exist_ok=True)


def connect_starrocks():
    return pymysql.connect(**STARROCKS_CONFIG)


def sr_query(sql, params=None):
    conn = connect_starrocks()
    try:
        df = pd.read_sql(sql, conn, params=params)
    finally:
        conn.close()
    return df


def remove_display_log_prophet():
    logger = logging.getLogger('cmdstanpy')
    logger.addHandler(logging.NullHandler())
    logger.propagate = False
    logger.setLevel(logging.CRITICAL)


def concat_dfs(path):
    file_lst = sorted(glob.glob(f"{path}*"))
    if not file_lst:
        return pd.DataFrame()
    frames = [pd.read_csv(f, sep=';', dtype={'mid': 'str'}) for f in file_lst]
    df = pd.concat(frames).sort_values('mid').reset_index(drop=True)
    return df


# ══════════════════════════════════════════════════════════════════════
# CHECKPOINT / MANIFEST (idempotency)
# ══════════════════════════════════════════════════════════════════════

def _manifest_path(run_dir):
    return os.path.join(run_dir, "manifest.json")


def load_manifest(run_dir):
    p = _manifest_path(run_dir)
    if os.path.exists(p):
        with open(p) as f:
            return json.load(f)
    return {}


def save_manifest(run_dir, data):
    makedir(run_dir)
    with open(_manifest_path(run_dir), "w") as f:
        json.dump(data, f, indent=2, default=str)


def mark_stage_done(run_dir, stage, extra=None):
    p = _manifest_path(run_dir)
    m = load_manifest(run_dir)
    m.setdefault("stages_done", [])
    if stage not in m["stages_done"]:
        m["stages_done"].append(stage)
    if extra:
        m.update(extra)
    tmp = p + ".tmp"
    with open(tmp, "w") as f:
        json.dump(m, f, indent=2, default=str)
    os.replace(tmp, p)


def is_stage_done(run_dir, stage):
    m = load_manifest(run_dir)
    return stage in m.get("stages_done", [])


def get_resume_batch(run_dir, param):
    """Get the last completed batch number for a given prediction param."""
    m = load_manifest(run_dir)
    return m.get("last_batch", {}).get(param, 0)


def set_resume_batch(run_dir, param, batch_num):
    # Atomic write: each param writes to its own key, so parallel processes don't conflict
    p = _manifest_path(run_dir)
    m = load_manifest(run_dir)
    m.setdefault("last_batch", {})[param] = batch_num
    tmp = p + ".tmp"
    with open(tmp, "w") as f:
        json.dump(m, f, indent=2, default=str)
    os.replace(tmp, p)  # atomic on both Linux and Windows


def get_dir_name(run_dir):
    m = load_manifest(run_dir)
    return m.get("dir_name")


# ══════════════════════════════════════════════════════════════════════
# DATE HELPERS
# ══════════════════════════════════════════════════════════════════════

def compute_prediction_dates(input_year, input_month, train_years=2):
    """Compute all derived dates for predicting a full month.

    Args:
        input_year: target prediction year (e.g. 2026)
        input_month: target prediction month (e.g. 5 = May)
        train_years: how many years of history to train on

    Example: input_year=2026, input_month=5
        → predict 2026-05-01 to 2026-05-31
        → train on 2024-04-30 to 2026-04-30 (2 years)
    """
    # Last day of the target prediction month
    pred_days = calendar.monthrange(input_year, input_month)[1]
    sum_area_from_date = date.datetime(input_year, input_month, 1)
    sum_area_to_date = date.datetime(input_year, input_month, pred_days)

    # Training cutoff = last day of previous month
    pred_prev = sum_area_from_date - relativedelta(days=1)
    max_train_date = date.datetime(pred_prev.year, pred_prev.month, pred_prev.day)

    # Minimum training date = train_years before max_train_date
    min_train_date = max_train_date - relativedelta(years=train_years)

    # Last 30 days window (for historical summary stats)
    last_month_date = max_train_date - relativedelta(months=1)

    # Sentinel row date (one day before max_train_date)
    append_max_date = max_train_date - pd.Timedelta(days=1)

    return {
        "max_train_date": max_train_date,
        "sum_area_from_date": sum_area_from_date,
        "sum_area_to_date": sum_area_to_date,
        "min_train_date": min_train_date,
        "last_month_date": last_month_date,
        "append_max_date": append_max_date,
    }


def build_dir_name(dates):
    d = dates
    return "thermal_paper_lte_{}_sum_area_{}_to_{}".format(
        d["max_train_date"].strftime("%Y%m%d"),
        d["sum_area_from_date"].strftime("%Y%m%d"),
        d["sum_area_to_date"].strftime("%Y%m%d"),
    )


def ramadan_shawwal(curr_date=None):
    if curr_date is None:
        curr_date = date.datetime.today()
    curr_year = curr_date.year
    hijri_year_9 = Gregorian(curr_year, 1, 1).to_hijri().year
    while True:
        g = Hijri(hijri_year_9, 9, 1).to_gregorian()
        gregorian_date_9 = date.datetime(g.year, g.month, g.day)
        if gregorian_date_9 > curr_date:
            break
        hijri_year_9 += 1
    hijri_year_10 = Gregorian(curr_year, 1, 1).to_hijri().year
    while True:
        g = Hijri(hijri_year_10, 10, 1).to_gregorian()
        gregorian_date_10 = date.datetime(g.year, g.month, g.day)
        if gregorian_date_10 > curr_date:
            break
        hijri_year_10 += 1
    return {"ramadan_start": gregorian_date_9, "shawwal_start": gregorian_date_10}


# ══════════════════════════════════════════════════════════════════════
# STAGE 0: REFERENCE DATA (from StarRocks)
# ══════════════════════════════════════════════════════════════════════

def fetch_population():
    """Replace ES scan of logstash-population with StarRocks SQL.
    is_pameran: SUBSTRING(mid, 6, 4) = '1989' (ES runtime mapping equivalent)
    """
    log("Fetching population from StarRocks")
    sql = f"""
        SELECT mid, tid, poi, serial_number,
               ticket_install_team, ticket_latest_team,
               merchant_name, merchant_criteria, store_code, store_name,
               store_address, store_postal, store_contact, store_pic,
               provinsi, kab_kot, kanwil, installed_date, is_production,
               terminal_stok_id, ticket_install_id
        FROM {TBL_POPULATION}
        WHERE installed_date IS NOT NULL
          AND ticket_dismantle_date_created IS NULL
          AND SUBSTRING(mid, 6, 4) != '1989'
    """
    df = sr_query(sql)
    df = df.rename(columns={"ticket_install_id": "id_sub_ticket"})
    if not df.empty:
        df["installed_date"] = pd.to_datetime(df["installed_date"])
    log(f"Population rows: {len(df)}")
    return df


def fetch_unique_mids():
    """Replace ES scan of transform-transaction_summary_thermal_paper_unique_mid."""
    log("Fetching unique MID list from StarRocks")
    sql = f"SELECT DISTINCT mid FROM {TBL_TRANSACTION} WHERE mid IS NOT NULL ORDER BY mid"
    df = sr_query(sql)
    log(f"Unique MIDs: {len(df)}")
    return df


def fetch_mid_behavior():
    """Replace ES partitioned aggs with a single StarRocks GROUP BY.
    Gets last month's print/reprint/transaction/settlement/gqr per MID,
    plus latest qr_is_included.
    """
    log("Fetching MID behavior from StarRocks")
    sql = f"""
        SELECT mid,
               SUM(sum_of_print_count) AS sum_of_print_count,
               SUM(number_of_print_count) AS number_of_print_count,
               SUM(number_of_reprint) AS number_of_reprint,
               SUM(number_of_transaction) AS number_of_transaction,
               SUM(number_of_settlement) AS number_of_settlement,
               SUM(number_of_generate_qr) AS number_of_generate_qr,
               MAX(qr_is_included) AS qr_is_included
        FROM {TBL_TRANSACTION}
        WHERE created_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH)
          AND created_date < CURRENT_DATE()
        GROUP BY mid
    """
    df = sr_query(sql)
    for col in ["sum_of_print_count", "number_of_print_count", "number_of_reprint",
                "number_of_transaction", "number_of_settlement", "number_of_generate_qr"]:
        if col in df.columns:
            df[col] = df[col].fillna(0).astype(int)
    if "qr_is_included" in df.columns:
        df["qr_is_included"] = df["qr_is_included"].replace("9999", np.nan)
    log(f"MID behavior rows: {len(df)}")
    return df


def fetch_global_trend_data(min_train_date=None, max_train_date=None):
    """Replace ES date_histogram aggs with StarRocks daily aggregation.
    Filters to training window if dates provided.
    """
    log("Fetching global trend data from StarRocks")
    where = []
    params = []
    if min_train_date:
        where.append("created_date >= %s")
        params.append(min_train_date.strftime("%Y-%m-%d"))
    if max_train_date:
        where.append("created_date <= %s")
        params.append(max_train_date.strftime("%Y-%m-%d"))
    where_clause = ("WHERE " + " AND ".join(where)) if where else ""
    sql = f"""
        SELECT DATE(created_date) AS ds,
               SUM(number_of_transaction) AS sum_trx,
               SUM(number_of_settlement) AS sum_settlement,
               SUM(number_of_generate_qr) AS sum_gen_qr,
               COUNT(*) AS doc_count
        FROM {TBL_TRANSACTION}
        {where_clause}
        GROUP BY DATE(created_date)
        ORDER BY ds
    """
    df = sr_query(sql)
    df["ds"] = pd.to_datetime(df["ds"])
    log(f"Global trend days: {len(df)}")
    return df


def fetch_per_mid_data(all_mid, max_train_date, min_train_date=None):
    """Bulk fetch all per-MID daily data in one query instead of per-MID ES scans.
    Filters to the training window [min_train_date, max_train_date].
    Returns DataFrame with columns: created_date, mid, number_of_transaction,
    number_of_settlement, number_of_generate_qr
    """
    log(f"Fetching per-MID transaction data for {len(all_mid)} MIDs")
    placeholders = ",".join(["%s"] * len(all_mid))
    if min_train_date:
        sql = f"""
            SELECT created_date, mid,
                   number_of_transaction, number_of_settlement, number_of_generate_qr
            FROM {TBL_TRANSACTION}
            WHERE mid IN ({placeholders})
              AND created_date >= %s
              AND created_date <= %s
            ORDER BY mid, created_date
        """
        params = list(all_mid) + [min_train_date.strftime("%Y-%m-%d"), max_train_date.strftime("%Y-%m-%d")]
    else:
        sql = f"""
            SELECT created_date, mid,
                   number_of_transaction, number_of_settlement, number_of_generate_qr
            FROM {TBL_TRANSACTION}
            WHERE mid IN ({placeholders})
              AND created_date <= %s
            ORDER BY mid, created_date
        """
        params = list(all_mid) + [max_train_date.strftime("%Y-%m-%d")]
    df = sr_query(sql, params=params)
    if not df.empty:
        df["created_date"] = pd.to_datetime(df["created_date"])
    log(f"Per-MID data rows: {len(df)}")
    return df


def build_holiday_df():
    """Build Idul Fitri holiday dates (pure computation, no data source needed)."""
    dates = []
    for i in range(2020, 2076):
        hijri_year = Gregorian(i, 1, 1).to_hijri().year
        g = Hijri(hijri_year, 10, 1).to_gregorian()
        dates.append(g)
    holiday = pd.DataFrame({
        "holiday": "idul_fitri",
        "ds": dates,
        "lower_window": -30,
        "upper_window": 14,
    }).sort_values("ds").reset_index(drop=True)
    return holiday


def stage_reference_data(run_dir, dates):
    if is_stage_done(run_dir, "reference_data"):
        log("Stage reference_data already done, skipping")
        return

    pop = fetch_population()
    pop.to_csv(os.path.join(BASE_DIR, "mid_and_pop", "pop.csv"), sep=";", index=False)

    mid_df = fetch_unique_mids()
    mid_df.to_csv(os.path.join(BASE_DIR, "mid_and_pop", "mid.csv"), sep=";", index=False)

    behavior = fetch_mid_behavior()
    behavior.to_csv(os.path.join(BASE_DIR, "additional_info", "mid_behavior_aggregated.csv"), sep=";", index=False)

    trend = fetch_global_trend_data(dates.get("min_train_date"), dates["max_train_date"])
    # Save for each param
    d = dates
    suffix = d["sum_area_to_date"].strftime("%Y%m")
    for param_field, param_name in [("sum_trx", "transaction"), ("sum_settlement", "settlement"), ("sum_gen_qr", "generate_qr")]:
        df_gt = trend.copy()
        df_gt["y"] = df_gt[param_field] / df_gt["doc_count"]
        df_gt = df_gt[["ds", "y"]]
        # Filter to training window
        df_gt_train = df_gt[df_gt["ds"] <= d["max_train_date"]]
        if d.get("min_train_date"):
            df_gt_train = df_gt_train[df_gt_train["ds"] >= d["min_train_date"]]
        if len(df_gt_train) > 10:
            remove_display_log_prophet()
            hd = build_holiday_df()
            model = Prophet(holidays=hd)
            model.fit(df_gt_train)
            future = model.make_future_dataframe(periods=PREDICT_X_DAYS)
            predicted = model.predict(future)
            predicted.to_csv(
                os.path.join(BASE_DIR, "additional_info", "global_trends", f"global_trend-{suffix}-{param_name}.csv"),
                sep=";", index=False
            )

    hd = build_holiday_df()
    hd.to_csv(os.path.join(BASE_DIR, "additional_info", "holiday.csv"), sep=";", index=False)

    mark_stage_done(run_dir, "reference_data")
    log("Stage reference_data complete")


# ══════════════════════════════════════════════════════════════════════
# STAGE 1-3: PREDICTION (per param)
# ══════════════════════════════════════════════════════════════════════

def _prepare_mid_series(raw_data, mid, global_param, dates):
    """Prepare a single MID's time series for Prophet. Returns (df, is_special_case)."""
    d = dates
    mid_data = raw_data[raw_data["mid"] == mid].copy()
    if mid_data.empty:
        return None, False

    col_map = {
        "transaction_count": {"y_col": "number_of_transaction", "other_cols": ["number_of_settlement", "number_of_generate_qr"]},
        "settlement_count":  {"y_col": "number_of_settlement", "other_cols": ["number_of_transaction", "number_of_generate_qr"]},
        "generate_qr_count": {"y_col": "number_of_generate_qr",  "other_cols": ["number_of_transaction", "number_of_settlement"]},
    }[global_param]

    df = pd.DataFrame({
        "ds": mid_data["created_date"],
        "mid": mid,
        "y": mid_data[col_map["y_col"]].astype(float),
    })
    for i, oc in enumerate(col_map["other_cols"]):
        df[["settlement_count", "generate_qr_count", "transaction_count"][i]] = mid_data[oc].astype(float) if oc in mid_data.columns else 0.0

    # Append max_date row if not present
    append_str = d["append_max_date"].strftime("%Y-%m-%d")
    if append_str not in df["ds"].dt.strftime("%Y-%m-%d").values:
        extra = pd.DataFrame({"ds": [d["append_max_date"]], "mid": [mid], "y": [0.0],
                              "settlement_count": [0.0], "generate_qr_count": [0.0], "transaction_count": [0.0]})
        df = pd.concat([df, extra])

    df = df.sort_values("ds").reset_index(drop=True)

    # Inject missing dates (asfreq)
    df.index = df["ds"]
    df = df.asfreq("D")
    df = df.reset_index(drop=True)
    df["mid"] = mid
    df[["settlement_count", "generate_qr_count", "transaction_count", "y"]] = df[["settlement_count", "generate_qr_count", "transaction_count", "y"]].fillna(0)
    df["day_count"] = 1

    is_special = df["ds"].min() > d["max_train_date"] - pd.Timedelta(days=MIN_DATA_DAYS - 1)
    return df, is_special


def _predict_one_mid(df, mid, global_param, dates, holiday_df):
    """Run Prophet for one MID, return sum_area row."""
    d = dates
    train = df[df["ds"] <= d["max_train_date"]][["ds", "y"]].copy()

    model = Prophet(holidays=holiday_df)
    model.fit(train)
    future = model.make_future_dataframe(periods=PREDICT_X_DAYS)
    predicted = model.predict(future)

    forecast = predicted[(predicted["ds"] >= d["sum_area_from_date"]) & (predicted["ds"] <= d["sum_area_to_date"])][["ds", "yhat", "yhat_upper", "yhat_lower"]].copy()
    forecast["mid"] = mid

    for i in SUM_CHOICE:
        if i <= 50:
            forecast[i] = forecast["yhat_lower"] + (forecast["yhat"] - forecast["yhat_lower"]) * i / 50
        else:
            forecast[i] = forecast["yhat"] + (forecast["yhat_upper"] - forecast["yhat"]) * (i - 50) / 50

    row = {"mid": mid, "tag": "normal_case"}
    for j in SUM_CHOICE:
        row[f"sum_{j}"] = forecast[j].sum()

    train_y = df["y"][df["ds"] <= d["max_train_date"]]
    row["historical_sum_actual_all"] = train_y.sum()
    row["historical_sum_actual_last_one_month"] = df["y"][(df["ds"] >= d["last_month_date"]) & (df["ds"] <= d["max_train_date"])].sum()
    row["historical_day_first_log"] = df["ds"][df["ds"] <= d["max_train_date"]].min()
    row["historical_day_last_log"] = df["ds"][df["ds"] <= d["max_train_date"]].max()
    row["historical_day_active"] = int(df["day_count"][df["ds"] <= d["max_train_date"]].sum())
    return pd.DataFrame([row])


def _special_case_one_mid(df, mid, dates):
    """Handle MID with insufficient data (< MIN_DATA_DAYS)."""
    d = dates
    train_y = df["y"][df["ds"] <= d["max_train_date"]]
    total = train_y.sum()
    diff_days = max((d["max_train_date"] - df["ds"].min()).days + 1, 1)
    prorated = total * (30 / diff_days)

    row = {"mid": mid, "tag": "special_case"}
    row["historical_sum_actual_all"] = total
    row["historical_sum_actual_last_one_month"] = df["y"][(df["ds"] >= d["last_month_date"]) & (df["ds"] <= d["max_train_date"])].sum()
    row["historical_day_first_log"] = df["ds"][df["ds"] <= d["max_train_date"]].min()
    row["historical_day_last_log"] = df["ds"][df["ds"] <= d["max_train_date"]].max()
    row["historical_day_active"] = int(df["day_count"][df["ds"] <= d["max_train_date"]].sum())
    for i in SUM_CHOICE:
        row[f"sum_{i}"] = prorated
    return pd.DataFrame([row])


def stage_predict(run_dir, dates, global_param):
    """Run prediction for one param with checkpoint-based resume."""
    stage_name = f"predict_{global_param.replace('_count', '')}"
    if is_stage_done(run_dir, stage_name):
        log(f"Stage {stage_name} already done, skipping")
        return

    remove_display_log_prophet()
    dir_name = get_dir_name(run_dir)
    steps_dir = os.path.join(BASE_DIR, dir_name, f"steps_batch_size_{BATCH_SIZE}_param_{global_param}")
    makedir(steps_dir)

    pop = pd.read_csv(os.path.join(BASE_DIR, "mid_and_pop", "pop.csv"), sep=";", dtype=str)
    mid_df = pd.read_csv(os.path.join(BASE_DIR, "mid_and_pop", "mid.csv"), sep=";", dtype=str)
    pop_mids = set(pop["mid"].unique())
    all_mid = sorted(set(mid_df["mid"]) & pop_mids)
    log(f"{stage_name}: {len(all_mid)} MIDs to predict")

    holiday_df = pd.read_csv(os.path.join(BASE_DIR, "additional_info", "holiday.csv"), sep=";")
    holiday_df["ds"] = pd.to_datetime(holiday_df["ds"])

    # Bulk fetch all data once (filtered to 2-year training window)
    raw_data = fetch_per_mid_data(all_mid, dates["max_train_date"], dates.get("min_train_date"))

    step_total = int(np.ceil(len(all_mid) / BATCH_SIZE))
    last_done = get_resume_batch(run_dir, global_param)
    if last_done > 0:
        log(f"Resuming {global_param} from batch {last_done + 1}/{step_total}")

    start_total = time.time()
    step_count = last_done

    for batch_idx in range(last_done, step_total):
        batch_start = batch_idx * BATCH_SIZE
        batch_end = min(batch_start + BATCH_SIZE, len(all_mid))
        mid_batch = all_mid[batch_start:batch_end]

        df_lst = []
        for mid in mid_batch:
            df_mid, is_special = _prepare_mid_series(raw_data, mid, global_param, dates)
            if df_mid is None:
                continue
            if is_special:
                df_lst.append(_special_case_one_mid(df_mid, mid, dates))
            else:
                df_lst.append(_predict_one_mid(df_mid, mid, global_param, dates, holiday_df))

        if df_lst:
            df_batch = pd.concat(df_lst).reset_index(drop=True)
        else:
            df_batch = pd.DataFrame()

        step_count += 1
        step_file = os.path.join(steps_dir, f"step_{step_count}_of_{step_total}.csv")
        df_batch.to_csv(step_file, sep=";", index=False)

        set_resume_batch(run_dir, global_param, step_count)
        elapsed = time.time() - start_total
        log(f"  {stage_name} batch {step_count}/{step_total} ({batch_end - batch_start} MIDs) elapsed={elapsed:.1f}s")

    # Concatenate all steps
    ready_dir = os.path.join(BASE_DIR, "ready_to_merge_with_population")
    makedir(ready_dir)
    df_final = concat_dfs(os.path.join(steps_dir, "step_"))
    df_final["dir_name"] = f"{dir_name}/steps_batch_size_{BATCH_SIZE}_param_{global_param}"
    df_final["max_train_date"] = dates["max_train_date"]

    param_short = global_param.replace("_count", "")
    df_final.to_csv(os.path.join(ready_dir, f"raw_{global_param}.csv"), sep=";", index=False)
    df_final.to_csv(os.path.join(BASE_DIR, dir_name, f"raw_{global_param}_{dir_name}.csv"), sep=";", index=False)

    # Clean up steps
    shutil.rmtree(steps_dir, ignore_errors=True)

    mark_stage_done(run_dir, stage_name)
    log(f"Stage {stage_name} complete")


# ══════════════════════════════════════════════════════════════════════
# STAGE 4: TRANSFORM (merge 3 predictions → paper rolls)
# ══════════════════════════════════════════════════════════════════════

def stage_transform(run_dir, dates):
    if is_stage_done(run_dir, "transform"):
        log("Stage transform already done, skipping")
        return

    dir_name = get_dir_name(run_dir)
    pop = pd.read_csv(os.path.join(BASE_DIR, "mid_and_pop", "pop.csv"), sep=";", dtype=str)
    pop_sorted = pop[["mid", "tid", "poi", "serial_number", "merchant_name", "merchant_criteria",
                       "store_name", "store_address", "provinsi", "kab_kot", "kanwil",
                       "installed_date", "ticket_latest_team"]].rename(columns={"ticket_latest_team": "team"})
    pop_sorted["installed_date"] = pd.to_datetime(pop_sorted["installed_date"])

    tid_count = pop_sorted[["mid"]].copy()
    tid_count["tid_count"] = 1
    tid_count = tid_count.groupby("mid").count().reset_index()
    pop_tid = pd.merge(pop_sorted, tid_count, on="mid", how="left")

    ready_dir = os.path.join(BASE_DIR, "ready_to_merge_with_population")
    df_trx = pd.read_csv(os.path.join(ready_dir, "raw_transaction_count.csv"), sep=";", dtype={"mid": "str"})
    df_stl = pd.read_csv(os.path.join(ready_dir, "raw_settlement_count.csv"), sep=";", dtype={"mid": "str"})
    df_gqr = pd.read_csv(os.path.join(ready_dir, "raw_generate_qr_count.csv"), sep=";", dtype={"mid": "str"})

    for df_src, col_name in [(df_trx, "forecast_transaction"), (df_stl, "forecast_settlement"), (df_gqr, "forecast_generate_qr")]:
        df_src.rename(columns={f"sum_{SUM_CHOICE[0]}": col_name}, inplace=True)
        df_src.loc[df_src[col_name] < 0, col_name] = 0.0
        df_src[col_name] = np.ceil(df_src[col_name])
        df_src.drop_duplicates(inplace=True)

    merged = pop_tid.merge(df_trx[["mid", "forecast_transaction"]], on="mid", how="left") \
                    .merge(df_stl[["mid", "forecast_settlement"]], on="mid", how="left") \
                    .merge(df_gqr[["mid", "forecast_generate_qr"]], on="mid", how="left")

    merged["forecast_transaction"] = merged["forecast_transaction"].fillna(1.0)
    merged["forecast_settlement"] = merged["forecast_settlement"].fillna(1.0)
    merged["forecast_generate_qr"] = merged["forecast_generate_qr"].fillna(1.0)

    bvr = pd.read_csv(os.path.join(BASE_DIR, "additional_info", "mid_behavior_aggregated.csv"), sep=";", dtype={"mid": "str", "qr_is_included": "str"})
    bvr = bvr.drop(["record_count", "number_of_settlement", "number_of_generate_qr"], axis=1, errors="ignore")
    bvr["qr_is_included"] = bvr["qr_is_included"].map({"1": True, "0": False})

    merged = merged.merge(bvr, on="mid", how="left")
    merged["print_behavior"] = np.ceil((merged["sum_of_print_count"] / merged["number_of_print_count"]) * 2) / 2
    merged.loc[(merged["sum_of_print_count"] == 0) & (merged["number_of_print_count"] == 0), "print_behavior"] = DEFAULT_PRINT_BEHAVIOR
    merged["reprint_behavior"] = merged["number_of_reprint"] / merged["number_of_transaction"]
    merged.loc[merged["qr_is_included"].isna(), "qr_is_included"] = True
    merged = merged.drop(["sum_of_print_count", "number_of_print_count", "number_of_reprint", "number_of_transaction"], axis=1, errors="ignore")
    merged["print_behavior"] = merged["print_behavior"].fillna(DEFAULT_PRINT_BEHAVIOR)
    merged["reprint_behavior"] = merged["reprint_behavior"].fillna(DEFAULT_REPRINT_BEHAVIOR)
    merged.loc[np.isinf(merged["reprint_behavior"]), "reprint_behavior"] = DEFAULT_REPRINT_BEHAVIOR

    # Divide by TID count
    for col in ["forecast_transaction", "forecast_settlement", "forecast_generate_qr"]:
        merged[col] = np.ceil(merged[col] / merged["tid_count"])

    # Paper consumption calculations
    merged["transaction_brimo_qr_included_cm"] = np.ceil(merged["forecast_transaction"] * CM_TRX_WITH_BRIMO)
    merged.loc[merged["qr_is_included"] == False, "transaction_brimo_qr_included_cm"] = np.ceil(merged["forecast_transaction"] * CM_TRX_NO_BRIMO)
    merged["transaction_print_effect_cm"] = np.ceil(merged["print_behavior"] * merged["transaction_brimo_qr_included_cm"])

    def _reprint_effect(row):
        cm = CM_TRX_WITH_BRIMO if row["qr_is_included"] else CM_TRX_NO_BRIMO
        rate = min(row["reprint_behavior"], MAX_REPRINT_BEHAVIOR)
        return np.ceil(np.ceil(row["forecast_transaction"] * rate) * cm)
    merged["transaction_reprint_effect_cm"] = merged.apply(_reprint_effect, axis=1)

    merged["transaction_cm"] = merged["transaction_print_effect_cm"] + merged["transaction_reprint_effect_cm"]
    merged["settlement_cm"] = np.ceil(merged["forecast_settlement"] * CM_SETTLEMENT)
    merged["generate_qr_cm"] = np.ceil(merged["forecast_generate_qr"] * CM_GENERATE_QR)
    merged["prediction_cm"] = merged["transaction_cm"] + merged["settlement_cm"] + merged["generate_qr_cm"]
    merged["prediction_rolls_before"] = np.ceil(merged["prediction_cm"] / ONE_ROLL_TOTAL_CM)
    merged["pm_needs_rolls"] = merged["prediction_rolls_before"]
    merged.loc[(merged["prediction_rolls_before"] == 0) | merged["prediction_rolls_before"].isna(), "pm_needs_rolls"] = DEFAULT_PM_NEEDS_ROLLS
    merged.loc[merged["pm_needs_rolls"] >= 100, "pm_needs_rolls"] = np.ceil(merged["pm_needs_rolls"] / 5) * 5
    merged["prediction_rolls"] = merged["pm_needs_rolls"]

    # Metadata columns
    now = (date.datetime.now() + pd.Timedelta(hours=7)).strftime("%Y-%m-%d %H:%M:%S.000")
    merged["mark"] = dir_name.replace("_", "")[-36:]
    merged["sum_choice"] = SUM_CHOICE[0]
    merged["one_roll_total_cm"] = ONE_ROLL_TOTAL_CM
    merged["created_at"] = now
    merged["updated_at"] = now
    merged["prediction_for_year"] = str(dates["sum_area_to_date"].year)
    merged["prediction_for_month"] = str(dates["sum_area_to_date"].month)

    # Tidy up
    for c in merged.select_dtypes(include="object").columns:
        if c != "qr_is_included":
            merged[c] = merged[c].fillna("-")
    for c in merged.select_dtypes(include="float").columns:
        if c not in ["print_behavior", "reprint_behavior"]:
            merged[c] = merged[c].fillna(0).astype(int)
    for c in merged.select_dtypes(include="datetime").columns:
        merged[c] = merged[c].fillna(date.datetime(2000, 1, 1))
    merged["qr_is_included"] = merged["qr_is_included"].fillna(True)
    merged["print_behavior"] = merged["print_behavior"].fillna(DEFAULT_PRINT_BEHAVIOR)

    for c in ["cm_transaction_with_brimo", "cm_transaction_with_no_brimo", "cm_generate_qr", "cm_settlement",
              "default_print_behavior", "default_reprint_behavior", "default_pm_needs_rolls"]:
        merged[c.replace("cm_", "cm_")] = {"cm_transaction_with_brimo": CM_TRX_WITH_BRIMO, "cm_transaction_with_no_brimo": CM_TRX_NO_BRIMO,
                                            "cm_generate_qr": CM_GENERATE_QR, "cm_settlement": CM_SETTLEMENT,
                                            "default_print_behavior": DEFAULT_PRINT_BEHAVIOR, "default_reprint_behavior": DEFAULT_REPRINT_BEHAVIOR,
                                            "default_pm_needs_rolls": DEFAULT_PM_NEEDS_ROLLS}[c]

    output_cols = [
        "mid", "poi", "tid", "serial_number", "merchant_name", "merchant_criteria",
        "store_name", "store_address", "provinsi", "kab_kot", "kanwil", "installed_date",
        "team", "tid_count", "forecast_transaction", "forecast_generate_qr", "forecast_settlement",
        "print_behavior", "reprint_behavior", "qr_is_included",
        "transaction_brimo_qr_included_cm", "transaction_print_effect_cm", "transaction_reprint_effect_cm",
        "transaction_cm", "generate_qr_cm", "settlement_cm", "prediction_cm",
        "prediction_rolls_before", "pm_needs_rolls", "prediction_rolls",
        "prediction_for_year", "prediction_for_month", "mark", "sum_choice", "one_roll_total_cm",
        "cm_transaction_with_brimo", "cm_transaction_with_no_brimo", "cm_generate_qr", "cm_settlement",
        "default_print_behavior", "default_reprint_behavior", "default_pm_needs_rolls",
        "created_at", "updated_at",
    ]
    merged = merged[[c for c in output_cols if c in merged.columns]]

    out_dir = os.path.join(BASE_DIR, dir_name)
    makedir(out_dir)
    merged.to_csv(os.path.join(out_dir, f"transformed_{dir_name}.csv"), sep=";", index=False)
    merged.to_csv(os.path.join(BASE_DIR, "adjusting_roll", "transformed.csv"), sep=";", index=False)

    mark_stage_done(run_dir, "transform")
    log("Stage transform complete")


# ══════════════════════════════════════════════════════════════════════
# STAGE 5: ADJUST (roll adjustment from ticket maintenance)
# ══════════════════════════════════════════════════════════════════════

def fetch_roll_adjustment():
    """Replace ES runtime_mapping + agg on ticket indices with StarRocks SQL."""
    log("Fetching roll adjustment from StarRocks")
    # tag_name contains number of rolls, e.g. "5 ROLL" → extract number
    sql = f"""
        SELECT tid,
               CEIL(SUM(CAST(REGEXP_EXTRACT(tag_name, '(\\\\d+)', 1) AS INT)) / 2) AS adjusted_avg
        FROM (
            SELECT tid, tag_name
            FROM {TBL_TICKET_MTC}
            WHERE kategori_name = 'Support Maintenance'
              AND parent_tag_name = 'THERMAL PAPER'
              AND date_created_sub_tiket >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 MONTH)
              AND date_created_sub_tiket < DATE_SUB(CURRENT_DATE(), INTERVAL 0 MONTH)
              AND tag_name IS NOT NULL
              AND tag_name RLIKE '\\\\d+'
            UNION ALL
            SELECT tid, tag_name
            FROM {TBL_TICKET_IMPL}
            WHERE kategori_name = 'Support Maintenance'
              AND parent_tag_name = 'THERMAL PAPER'
              AND date_created_sub_tiket >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 MONTH)
              AND date_created_sub_tiket < DATE_SUB(CURRENT_DATE(), INTERVAL 0 MONTH)
              AND tag_name IS NOT NULL
              AND tag_name RLIKE '\\\\d+'
        ) combined
        GROUP BY tid
    """
    df = sr_query(sql)
    if not df.empty:
        df["adjusted_avg"] = df["adjusted_avg"].fillna(0).astype(int)
    log(f"Roll adjustment TIDs: {len(df)}")
    return df


def stage_adjust(run_dir):
    if is_stage_done(run_dir, "adjust"):
        log("Stage adjust already done, skipping")
        return

    df_adj = fetch_roll_adjustment()
    df_adj.to_csv(os.path.join(BASE_DIR, "adjusting_roll", "sm_adjust.csv"), sep=";", index=False)

    df_transformed = pd.read_csv(os.path.join(BASE_DIR, "adjusting_roll", "transformed.csv"), sep=";", dtype={"mid": "str"})
    df_adj = df_adj[["tid", "adjusted_avg"]].copy()
    df_transformed["tid"] = df_transformed["tid"].astype(str)
    df_adj["tid"] = df_adj["tid"].astype(str)

    merged = df_transformed.merge(df_adj, on="tid", how="left")
    mask = merged["adjusted_avg"].notna()
    merged.loc[mask, "prediction_rolls"] = merged.loc[mask, "prediction_rolls"] + merged.loc[mask, "adjusted_avg"]
    merged.loc[mask, "pm_needs_rolls"] = merged.loc[mask, "pm_needs_rolls"] + merged.loc[mask, "adjusted_avg"]
    merged = merged.drop(columns=["adjusted_avg"])

    log(f"Adjusted TIDs: {mask.sum()}")
    merged.to_csv(os.path.join(BASE_DIR, "ready_to_insert_to_elk", "transformed_with_adjusting.csv"), sep=";", index=False)

    mark_stage_done(run_dir, "adjust")
    log("Stage adjust complete")


# ══════════════════════════════════════════════════════════════════════
# STAGE 6: OUTPUT (write to StarRocks)
# ══════════════════════════════════════════════════════════════════════

def stage_output(run_dir, output_table="gold.fms.thermal_paper_prediction"):
    """Write final predictions to StarRocks (replaces ES bulk index)."""
    if is_stage_done(run_dir, "output"):
        log("Stage output already done, skipping")
        return

    df = pd.read_csv(os.path.join(BASE_DIR, "ready_to_insert_to_elk", "transformed_with_adjusting.csv"), sep=";", dtype={"mid": "str"})
    df = df[df["installed_date"].notna()].replace(np.nan, "-")

    for col in ["installed_date", "created_at", "updated_at"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")

    log(f"Writing {len(df)} rows to StarRocks table {output_table}")
    conn = connect_starrocks()
    try:
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {output_table}")

        cols = ", ".join(df.columns)
        placeholders = ", ".join(["%s"] * len(df.columns))
        sql = f"INSERT INTO {output_table} ({cols}) VALUES ({placeholders})"

        batch_size = 500
        total = len(df)
        for start in range(0, total, batch_size):
            batch = df.iloc[start:start + batch_size]
            with conn.cursor() as cur:
                cur.executemany(sql, batch.values.tolist())
            conn.commit()
            log(f"  Inserted {min(start + batch_size, total)}/{total}")
    finally:
        conn.close()

    mark_stage_done(run_dir, "output")
    log("Stage output complete")


# ══════════════════════════════════════════════════════════════════════
# MAIN ENTRYPOINT
# ══════════════════════════════════════════════════════════════════════

def run_pipeline(input_year=None, input_month=None, train_years=2, output_table="gold.fms.thermal_paper_prediction"):
    """Run the full thermal paper prediction pipeline.

    Args:
        input_year: target prediction year (e.g. 2026). Default: current year.
        input_month: target prediction month (e.g. 5 for May). Default: current month.
        train_years: years of history to train Prophet on. Default: 2.
        output_table: StarRocks table to write final predictions to.
    """
    now = CurrDate()
    input_year = input_year or now.year_int()
    input_month = input_month or now.month_int()

    dates = compute_prediction_dates(input_year, input_month, train_years)
    dir_name = build_dir_name(dates)
    run_dir = os.path.join(BASE_DIR, dir_name)

    # Ensure subdirs exist
    for sub in ["mid_and_pop", "additional_info/global_trends", "ready_to_merge_with_population",
                "adjusting_roll", "ready_to_insert_to_elk", dir_name]:
        makedir(os.path.join(BASE_DIR, sub))

    # Initialize manifest
    m = load_manifest(run_dir)
    if not m.get("dir_name"):
        m["dir_name"] = dir_name
        m["max_train_date"] = str(dates["max_train_date"])
        m["min_train_date"] = str(dates["min_train_date"])
        m["sum_area_from"] = str(dates["sum_area_from_date"])
        m["sum_area_to"] = str(dates["sum_area_to_date"])
        m["train_years"] = train_years
        save_manifest(run_dir, m)

    log(f"Pipeline: {dir_name}")
    log(f"  Train: {dates['min_train_date'].strftime('%Y-%m-%d')} to {dates['max_train_date'].strftime('%Y-%m-%d')} ({train_years}y)")
    log(f"  Predict: {dates['sum_area_from_date'].strftime('%Y-%m-%d')} to {dates['sum_area_to_date'].strftime('%Y-%m-%d')}")

    stage_reference_data(run_dir, dates)

    predict_params = ["transaction_count", "settlement_count", "generate_qr_count"]
    # Filter to only params that aren't already done
    pending = [p for p in predict_params if not is_stage_done(run_dir, f"predict_{p.replace('_count', '')}")]

    if pending:
        log(f"Running {len(pending)} prediction stage(s) in parallel: {pending}")
        with ProcessPoolExecutor(max_workers=len(pending)) as pool:
            futures = {pool.submit(stage_predict, run_dir, dates, param): param for param in pending}
            for fut in as_completed(futures):
                param = futures[fut]
                try:
                    fut.result()
                except Exception as e:
                    log(f"ERROR in predict_{param}: {e}")
                    raise

    stage_transform(run_dir, dates)
    stage_adjust(run_dir)
    stage_output(run_dir, output_table)

    log("Pipeline complete!")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Thermal Paper Prediction Pipeline (StarRocks)")
    parser.add_argument("--year", type=int, default=None, help="Target prediction year (e.g. 2026)")
    parser.add_argument("--month", type=int, default=None, help="Target prediction month (e.g. 5 for May)")
    parser.add_argument("--train-years", type=int, default=2, help="Years of history to train on (default: 2)")
    parser.add_argument("--output-table", default="gold.fms.thermal_paper_prediction", help="StarRocks output table")
    parser.add_argument("--reset", action="store_true", help="Remove manifest and restart from scratch")
    args = parser.parse_args()

    if args.reset:
        now = CurrDate()
        dates = compute_prediction_dates(args.year or now.year_int(), args.month or now.month_int(), args.train_years)
        dir_name = build_dir_name(dates)
        manifest = os.path.join(BASE_DIR, dir_name, "manifest.json")
        if os.path.exists(manifest):
            os.remove(manifest)
            log(f"Removed manifest: {manifest}")

    run_pipeline(input_year=args.year, input_month=args.month, train_years=args.train_years, output_table=args.output_table)
