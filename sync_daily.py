#!/usr/bin/env python3
"""
A-share full-market daily bars sync.

Output (committed to repo):
  data/daily_latest.parquet  - rolling ROLLING_DAYS calendar days, all stocks
  data/universe.csv          - code / name / is_st snapshot
  data/meta.json             - last sync time, date range, row counts

Data contract (units):
  date     YYYY-MM-DD (pandas datetime)
  code     6-digit, e.g. 600519
  open/high/low/close  CNY, UNADJUSTED (adjust="")
  volume   手 (1 手 = 100 shares)
  amount   元
  turnover % (as quoted by eastmoney)
  pct_chg  % (exchange-official daily change; use this for limit-up detection,
            avoids all corporate-action / adjust headaches)

Limit-up detection guidance (consumer side):
  主板/others: pct_chg >= 9.5 ; ST: >= 4.5
  创业板(3xxxxx)/科创板(688xxx): >= 19.0
  北交所(8xxxxx/4xxxxx): >= 29.0
"""

import json
import os
import time
import datetime as dt
from concurrent.futures import ThreadPoolExecutor, as_completed

import akshare as ak
import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
PARQUET = os.path.join(DATA_DIR, "daily_latest.parquet")
UNIVERSE_CSV = os.path.join(DATA_DIR, "universe.csv")
META_JSON = os.path.join(DATA_DIR, "meta.json")
FAILED_TXT = os.path.join(DATA_DIR, "_failed.txt")

ROLLING_DAYS = 120          # keep last N calendar days in parquet
BACKFILL_START = "20190101"  # first-ever run starts here (covers pre-quant control)
MAX_WORKERS = 8
RETRIES = 3

os.makedirs(DATA_DIR, exist_ok=True)


def get_universe() -> pd.DataFrame:
    spot = ak.stock_zh_a_spot_em()
    df = spot[["代码", "名称"]].copy()
    df.columns = ["code", "name"]
    df["is_st"] = df["name"].str.contains(r"^\*?ST", regex=True)
    return df


def fetch_one(code: str, start: str, end: str):
    """Fetch daily history for one stock. Returns DataFrame or None."""
    for attempt in range(RETRIES):
        try:
            df = ak.stock_zh_a_hist(
                symbol=code, period="daily",
                start_date=start, end_date=end, adjust="",
            )
            if df is None or df.empty:
                return None
            df = df.rename(columns={
                "日期": "date", "开盘": "open", "收盘": "close",
                "最高": "high", "最低": "low", "成交量": "volume",
                "成交额": "amount", "换手率": "turnover", "涨跌幅": "pct_chg",
            })
            df["code"] = code
            df["date"] = pd.to_datetime(df["date"])
            return df[["date", "code", "open", "high", "low", "close",
                       "volume", "amount", "turnover", "pct_chg"]]
        except Exception:
            time.sleep(2 * (attempt + 1))
    return "FAILED:" + code


def main():
    today = dt.date.today()
    end = today.strftime("%Y%m%d")

    if os.path.exists(PARQUET):
        old = pd.read_parquet(PARQUET)
        max_date = pd.to_datetime(old["date"]).max().date()
        start = (max_date + dt.timedelta(days=1)).strftime("%Y%m%d")
        print(f"incremental sync: {start} -> {end} (existing rows {len(old)})")
    else:
        old = None
        start = BACKFILL_START
        print(f"first run, backfill: {start} -> {end}")

    universe = get_universe()
    universe.to_csv(UNIVERSE_CSV, index=False)
    print(f"universe: {len(universe)} stocks, ST={int(universe['is_st'].sum())}")

    if start > end:
        print("already up to date, nothing to fetch")
        new_all = None
    else:
        codes = universe["code"].tolist()
        frames, failed = [], []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futs = {ex.submit(fetch_one, c, start, end): c for c in codes}
            for i, fut in enumerate(as_completed(futs), 1):
                r = fut.result()
                if isinstance(r, str) and r.startswith("FAILED:"):
                    failed.append(r.split(":", 1)[1])
                elif r is not None:
                    frames.append(r)
                if i % 500 == 0:
                    print(f"  ... {i}/{len(codes)} done")
        print(f"fetched {len(frames)} stocks ok, {len(failed)} failed")
        with open(FAILED_TXT, "w") as f:
            f.write("\n".join(failed))
        new_all = pd.concat(frames, ignore_index=True) if frames else None

    if old is not None and new_all is not None:
        all_df = pd.concat([old, new_all], ignore_index=True)
    elif new_all is not None:
        all_df = new_all
    else:
        all_df = old

    all_df = all_df.drop_duplicates(subset=["date", "code"], keep="last")
    cutoff = pd.Timestamp(today - dt.timedelta(days=ROLLING_DAYS))
    all_df = all_df[pd.to_datetime(all_df["date"]) >= cutoff]
    all_df = all_df.sort_values(["date", "code"]).reset_index(drop=True)
    all_df.to_parquet(PARQUET, index=False)

    meta = {
        "synced_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "date_min": str(pd.to_datetime(all_df["date"]).min().date()),
        "date_max": str(pd.to_datetime(all_df["date"]).max().date()),
        "rows": int(len(all_df)),
        "stocks": int(all_df["code"].nunique()),
        "rolling_days": ROLLING_DAYS,
    }
    with open(META_JSON, "w") as f:
        json.dump(meta, f, indent=2, ensure_ascii=False)
    print("meta:", meta)


if __name__ == "__main__":
    main()
