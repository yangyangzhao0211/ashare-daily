#!/usr/bin/env python3
"""
A-share full-market daily bars DB (V3 - stable checkpointed version)

Design goals:
1. Direct Eastmoney HTTP API for historical daily bars.
2. Every HTTP request has a hard timeout.
3. Retry + exponential backoff.
4. Low concurrency to reduce rate-limit risk.
5. One historical year per workflow run.
6. Checkpoint every BATCH_SIZE stocks.
7. Failed stocks are retried on the next run.
8. Historical yearly partitions are never deleted.
9. Daily incremental sync after historical backfill is complete.
10. Best-effort delisted-stock coverage.

Layout:
  data/daily/YYYY.parquet
  data/universe.csv
  data/delist.csv
  data/meta.json
  data/state/YYYY.json
  data/_failed.txt

Columns:
  date, code, name, open, high, low, close,
  volume, amount, turnover, pct_chg, src,
  limit_up_approx, limit_down_approx

Prices are UNADJUSTED.

Eastmoney K-line fields:
  f51 date
  f52 open
  f53 close
  f54 high
  f55 low
  f56 volume
  f57 amount
  f58 amplitude
  f59 pct_chg
  f60 change
  f61 turnover

fqt=0 = unadjusted.
"""

import json
import os
import re
import sys
import time
import random
import datetime as dt
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import local

import requests
import pandas as pd


# ============================================================
# Configuration
# ============================================================

START_YEAR = 2019

# Keep this conservative. The problem is network stability,
# not CPU availability.
MAX_WORKERS = 4

# Number of stocks processed before writing a checkpoint.
BATCH_SIZE = 200

# HTTP timeout per request.
REQUEST_TIMEOUT = 10

# Retry count per stock.
RETRIES = 3

# Minimum delay before each HTTP request from each worker.
REQUEST_DELAY = 0.15

# Eastmoney API
EM_KLINE_URL = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
EM_LIST_URL = "https://push2.eastmoney.com/api/qt/clist/get"

EM_UT = "fa5fd1943c7b386f172d6893dbfba10b"

# A-share universe:
# Shanghai main + STAR
# Shenzhen main + ChiNext
# Beijing Stock Exchange
A_SHARE_FS = (
    "m:0+t:6,"
    "m:0+t:80,"
    "m:1+t:2,"
    "m:1+t:23,"
    "m:0+t:81+s:2048"
)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 "
        "(KHTML, like Gecko) "
        "Chrome/128.0 Safari/537.36"
    ),
    "Referer": "https://quote.eastmoney.com/",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
}

_thread_local = local()


# ============================================================
# Repository paths
# ============================================================

def find_repo_root():
    p = os.path.abspath(__file__)
    d = os.path.dirname(p)

    for _ in range(6):
        if os.path.isdir(os.path.join(d, ".github")):
            return d
        d = os.path.dirname(d)

    return os.getcwd()


ROOT = find_repo_root()

DATA_DIR = os.path.join(ROOT, "data")
DAILY_DIR = os.path.join(DATA_DIR, "daily")
STATE_DIR = os.path.join(DATA_DIR, "state")

UNIVERSE_CSV = os.path.join(DATA_DIR, "universe.csv")
DELIST_CSV = os.path.join(DATA_DIR, "delist.csv")
META_JSON = os.path.join(DATA_DIR, "meta.json")
FAILED_TXT = os.path.join(DATA_DIR, "_failed.txt")

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(DAILY_DIR, exist_ok=True)
os.makedirs(STATE_DIR, exist_ok=True)


# ============================================================
# HTTP session
# ============================================================

def get_session():
    """
    One requests.Session per worker thread.
    """
    if not hasattr(_thread_local, "session"):
        s = requests.Session()
        s.headers.update(HEADERS)
        _thread_local.session = s

    return _thread_local.session


def sleep_between_requests():
    time.sleep(REQUEST_DELAY + random.uniform(0.0, 0.10))


# ============================================================
# Utility
# ============================================================

def norm_code(x):
    m = re.search(r"(\d{6})", str(x or ""))
    return m.group(1) if m else ""


def is_st_name(name):
    return bool(re.match(r"^\*?ST", str(name or "")))


def market_id(code):
    """
    Eastmoney secid:
      Shanghai -> 1
      Shenzhen / Beijing -> 0
    """
    code = str(code).zfill(6)

    if code.startswith(("6", "68", "9")):
        return "1"

    return "0"


def secid(code):
    return f"{market_id(code)}.{str(code).zfill(6)}"


# ============================================================
# Universe
# ============================================================

def get_current_universe():
    """
    Get current A-share universe directly from Eastmoney.

    Includes:
      Shanghai main
      STAR
      Shenzhen main
      ChiNext
      Beijing Stock Exchange
    """

    params = {
        "pn": 1,
        "pz": 5000,
        "po": 1,
        "np": 1,
        "ut": EM_UT,
        "fltt": 2,
        "invt": 2,
        "fid": "f3",
        "fs": A_SHARE_FS,
        "fields": "f12,f14",
    }

    last_error = None

    for attempt in range(RETRIES):
        try:
            sleep_between_requests()

            r = get_session().get(
                EM_LIST_URL,
                params=params,
                timeout=REQUEST_TIMEOUT,
            )

            r.raise_for_status()
            data = r.json()

            diff = ((data.get("data") or {}).get("diff")) or []

            if not diff:
                raise RuntimeError(
                    "Eastmoney universe returned empty result"
                )

            rows = []

            for item in diff:
                code = norm_code(item.get("f12"))
                name = str(item.get("f14") or "")

                if code:
                    rows.append(
                        {
                            "code": code,
                            "name": name,
                            "is_st": is_st_name(name),
                        }
                    )

            df = pd.DataFrame(rows)

            if df.empty:
                raise RuntimeError("Universe dataframe is empty")

            df = df.drop_duplicates("code")
            df = df.sort_values("code").reset_index(drop=True)

            print(
                f"current universe: {len(df)} stocks "
                f"(ST={int(df['is_st'].sum())})"
            )

            return df

        except Exception as e:
            last_error = e
            print(
                f"universe attempt {attempt + 1}/{RETRIES} failed: {e}"
            )
            time.sleep(2 ** attempt)

    raise RuntimeError(
        f"Cannot obtain current A-share universe: {last_error}"
    )


# ============================================================
# Delisted stocks
# ============================================================

def get_delisted_best_effort():
    """
    Best-effort delisted stock retrieval.

    AkShare is executed in a separate subprocess so that a hanging
    AkShare request cannot freeze the main workflow.

    If this fails, the main historical sync continues.
    """

    script = r'''
import json
import akshare as ak
import pandas as pd

frames = []

try:
    sh = ak.stock_info_sh_delist("全部")
    sh = sh.rename(columns={
        "公司代码": "code",
        "公司简称": "name",
        "上市日期": "list_date",
        "暂停上市日期": "delist_date"
    })

    keep = [
        c for c in
        ["code", "name", "list_date", "delist_date"]
        if c in sh.columns
    ]

    if keep:
        frames.append(sh[keep])

except Exception:
    pass


try:
    sz = ak.stock_info_sz_delist("终止上市公司")

    rename = {}

    for c in sz.columns:
        s = str(c)

        if "代码" in s:
            rename[c] = "code"
        elif "简称" in s or "名称" in s:
            rename[c] = "name"
        elif "上市日期" in s:
            rename[c] = "list_date"
        elif "终止" in s and "日期" in s:
            rename[c] = "delist_date"

    sz = sz.rename(columns=rename)

    keep = [
        c for c in
        ["code", "name", "list_date", "delist_date"]
        if c in sz.columns
    ]

    if keep:
        frames.append(sz[keep])

except Exception:
    pass


if frames:
    df = pd.concat(frames, ignore_index=True)

    for c in ["code", "name", "list_date", "delist_date"]:
        if c not in df.columns:
            df[c] = None

    df["code"] = (
        df["code"]
        .astype(str)
        .str.extract(r"(\d{6})")[0]
    )

    df = df.dropna(subset=["code"])
    df = df.drop_duplicates("code")

    print(
        df[
            ["code", "name", "list_date", "delist_date"]
        ].to_json(orient="records", force_ascii=False)
    )

else:
    print("[]")
'''

    try:
        result = subprocess.run(
            [sys.executable, "-c", script],
            capture_output=True,
            text=True,
            timeout=90,
        )

        if result.returncode != 0:
            print(
                "delisted subprocess failed; continuing without it"
            )
            return pd.DataFrame(
                columns=[
                    "code",
                    "name",
                    "list_date",
                    "delist_date",
                ]
            )

        # Last non-empty line is JSON.
        lines = [
            x.strip()
            for x in result.stdout.splitlines()
            if x.strip()
        ]

        if not lines:
            return pd.DataFrame(
                columns=[
                    "code",
                    "name",
                    "list_date",
                    "delist_date",
                ]
            )

        records = json.loads(lines[-1])

        if not records:
            print("delisted: 0")
            return pd.DataFrame(
                columns=[
                    "code",
                    "name",
                    "list_date",
                    "delist_date",
                ]
            )

        df = pd.DataFrame(records)

        for c in ["list_date", "delist_date"]:
            df[c] = pd.to_datetime(
                df[c],
                errors="coerce"
            )

        print(f"delisted: {len(df)}")

        return df

    except subprocess.TimeoutExpired:
        print(
            "delisted lookup timed out after 90s; "
            "continuing without delisted data"
        )

    except Exception as e:
        print(
            f"delisted lookup failed: {e}; "
            "continuing without delisted data"
        )

    return pd.DataFrame(
        columns=[
            "code",
            "name",
            "list_date",
            "delist_date",
        ]
    )


# ============================================================
# Eastmoney historical K-line
# ============================================================

STD_COLS = [
    "date",
    "code",
    "name",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
    "turnover",
    "pct_chg",
    "src",
]


def fetch_em(code, start, end):
    """
    Direct Eastmoney daily K-line.

    fqt=0 = unadjusted.
    """

    params = {
        "secid": secid(code),
        "ut": EM_UT,
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": (
            "f51,f52,f53,f54,f55,f56,"
            "f57,f58,f59,f60,f61"
        ),
        "klt": 101,
        "fqt": 0,
        "beg": start,
        "end": end,
    }

    sleep_between_requests()

    r = get_session().get(
        EM_KLINE_URL,
        params=params,
        timeout=REQUEST_TIMEOUT,
    )

    r.raise_for_status()

    payload = r.json()

    data = payload.get("data") or {}
    klines = data.get("klines") or []

    if not klines:
        return None

    rows = []

    for line in klines:
        parts = str(line).split(",")

        if len(parts) < 11:
            continue

        rows.append(
            {
                "date": parts[0],
                "open": parts[1],
                "close": parts[2],
                "high": parts[3],
                "low": parts[4],
                "volume": parts[5],
                "amount": parts[6],
                "pct_chg": parts[8],
                "turnover": parts[10],
            }
        )

    if not rows:
        return None

    df = pd.DataFrame(rows)

    df["date"] = pd.to_datetime(
        df["date"],
        errors="coerce"
    )

    numeric_cols = [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
        "pct_chg",
        "turnover",
    ]

    for c in numeric_cols:
        df[c] = pd.to_numeric(
            df[c],
            errors="coerce"
        )

    df["code"] = code
    df["src"] = "em"

    return df[
        [
            "date",
            "code",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "amount",
            "turnover",
            "pct_chg",
            "src",
        ]
    ]


# ============================================================
# Per-stock fetch with timeout/retry
# ============================================================

def fetch_one(code, start, end):
    """
    Return:

      ("OK", dataframe)
      ("EMPTY", None)
      ("FAILED", error_message)

    Empty responses are retried once because Eastmoney can sometimes
    return HTTP 200 + empty klines during rate limiting.
    """

    last_error = None

    for attempt in range(RETRIES):

        try:
            df = fetch_em(
                code,
                start,
                end
            )

            if df is not None and not df.empty:
                return "OK", df

            # Empty response:
            # retry once before treating it as genuinely empty.
            if attempt < RETRIES - 1:
                wait = 1.5 * (attempt + 1)

                print(
                    f"  {code}: empty response, "
                    f"retrying in {wait:.1f}s"
                )

                time.sleep(wait)
                continue

            return "EMPTY", None

        except Exception as e:
            last_error = str(e)

            if attempt < RETRIES - 1:
                wait = (
                    1.5 * (2 ** attempt)
                    + random.uniform(0, 0.5)
                )

                time.sleep(wait)

    return "FAILED", last_error


# ============================================================
# Historical state
# ============================================================

def state_path(year):
    return os.path.join(
        STATE_DIR,
        f"{year}.json"
    )


def load_year_state(year):
    p = state_path(year)

    if os.path.exists(p):
        try:
            with open(
                p,
                "r",
                encoding="utf-8"
            ) as f:
                state = json.load(f)

            state.setdefault("done", [])
            state.setdefault("empty", [])
            state.setdefault("failed", [])

            return state

        except Exception:
            pass

    return {
        "year": year,
        "done": [],
        "empty": [],
        "failed": [],
        "complete": False,
    }


def save_year_state(year, state):
    p = state_path(year)

    tmp = p + ".tmp"

    with open(
        tmp,
        "w",
        encoding="utf-8"
    ) as f:
        json.dump(
            state,
            f,
            indent=2,
            ensure_ascii=False,
        )

    os.replace(tmp, p)


# ============================================================
# Meta
# ============================================================

def load_meta():
    if os.path.exists(META_JSON):

        try:
            with open(
                META_JSON,
                "r",
                encoding="utf-8"
            ) as f:
                return json.load(f)

        except Exception:
            pass

    return {
        "version": "V3",
        "years": {},
        "date_min": None,
        "date_max": None,
        "attempted_max": None,
        "total_rows": 0,
        "last_run": None,
    }


def save_meta(meta):
    tmp = META_JSON + ".tmp"

    with open(
        tmp,
        "w",
        encoding="utf-8"
    ) as f:
        json.dump(
            meta,
            f,
            indent=2,
            ensure_ascii=False,
        )

    os.replace(tmp, META_JSON)


# ============================================================
# Limit flags
# ============================================================

def limit_threshold(
    code,
    is_st,
    date
):
    """
    Approximate price-limit threshold.

    Main board:
      10%

    ST:
      5%

    STAR:
      20%

    ChiNext:
      20% from 2020-08-24

    Beijing:
      30% from 2021-11-15
    """

    d = pd.to_datetime(date).date()
    code = str(code)

    if is_st:
        return 5.0

    if code.startswith("688"):
        return 20.0

    if code.startswith(("300", "301")):
        if d >= dt.date(2020, 8, 24):
            return 20.0
        return 10.0

    if code.startswith(
        (
            "4",
            "8",
            "92",
        )
    ):
        if d >= dt.date(2021, 11, 15):
            return 30.0
        return 10.0

    return 10.0


def add_limit_flags(
    df,
    st_map
):
    if df.empty:
        return df

    df = df.copy()

    df["is_st"] = (
        df["code"]
        .map(st_map)
        .fillna(False)
    )

    thresholds = [
        limit_threshold(
            code,
            is_st,
            date,
        )
        for code, is_st, date in zip(
            df["code"],
            df["is_st"],
            df["date"],
        )
    ]

    thresholds = pd.Series(
        thresholds,
        index=df.index,
    )

    # Keep approximate flags intentionally conservative.
    df["limit_up_approx"] = (
        df["pct_chg"]
        >= (thresholds - 0.5)
    )

    df["limit_down_approx"] = (
        df["pct_chg"]
        <= -(thresholds - 0.5)
    )

    return df.drop(
        columns=["is_st"]
    )


# ============================================================
# Year partition
# ============================================================

def year_path(year):
    return os.path.join(
        DAILY_DIR,
        f"{year}.parquet"
    )


def upsert_year(
    year,
    new_df,
    st_map,
    name_map,
):
    """
    Merge new rows into yearly parquet.

    Existing rows are never deleted.
    """

    p = year_path(year)

    if new_df is None or new_df.empty:
        return 0

    df = new_df.copy()

    df["name"] = (
        df["code"]
        .map(name_map)
        .fillna(df.get("name", ""))
    )

    df = add_limit_flags(
        df,
        st_map,
    )

    if os.path.exists(p):
        old = pd.read_parquet(p)

        all_df = pd.concat(
            [old, df],
            ignore_index=True,
        )

    else:
        all_df = df

    all_df["date"] = pd.to_datetime(
        all_df["date"],
        errors="coerce",
    )

    all_df = (
        all_df
        .dropna(subset=["date"])
        .drop_duplicates(
            subset=["date", "code"],
            keep="last",
        )
        .sort_values(
            ["date", "code"]
        )
        .reset_index(drop=True)
    )

    all_df.to_parquet(
        p,
        index=False,
    )

    return len(df)


# ============================================================
# Batch fetch
# ============================================================

def fetch_batch(
    codes,
    start,
    end,
):
    """
    Fetch one batch concurrently.
    """

    results = []
    success = []
    empty = []
    failed = []

    with ThreadPoolExecutor(
        max_workers=MAX_WORKERS
    ) as executor:

        futures = {
            executor.submit(
                fetch_one,
                code,
                start,
                end,
            ): code
            for code in codes
        }

        for future in as_completed(futures):

            code = futures[future]

            try:
                status, payload = future.result()

            except Exception as e:
                status = "FAILED"
                payload = str(e)

            if status == "OK":

                results.append(payload)
                success.append(code)

            elif status == "EMPTY":

                empty.append(code)

            else:

                failed.append(code)

                print(
                    f"  FAILED {code}: {payload}"
                )

    if results:

        new_df = pd.concat(
            results,
            ignore_index=True,
        )

    else:

        new_df = pd.DataFrame()

    return (
        new_df,
        success,
        empty,
        failed,
    )


# ============================================================
# Historical backfill
# ============================================================

def get_next_incomplete_year(
    start_year,
    current_year,
    meta,
):
    for year in range(
        start_year,
        current_year + 1,
    ):

        p = year_path(year)

        info = meta["years"].get(
            str(year),
            {},
        )

        if (
            info.get("complete")
            and os.path.exists(p)
        ):
            continue

        return year

    return None


def backfill_one_year(
    year,
    universe,
    delisted,
    st_map,
    name_map,
    meta,
):
    """
    Process exactly ONE historical year.

    This is intentional.

    A single GitHub Action should not attempt
    2019 -> present in one run.
    """

    print("")
    print("=" * 70)
    print(f"BACKFILL YEAR {year}")
    print("=" * 70)

    y_start = f"{year}0101"
    y_end = f"{year}1231"

    state = load_year_state(year)

    done = set(state.get("done", []))
    empty = set(state.get("empty", []))
    failed_old = set(state.get("failed", []))

    # Current listed universe
    codes = set(
        universe["code"]
        .astype(str)
        .tolist()
    )

    # Add delisted companies active during this year.
    if (
        delisted is not None
        and not delisted.empty
    ):

        for _, row in delisted.iterrows():

            code = norm_code(
                row.get("code", "")
            )

            if not code:
                continue

            list_date = pd.to_datetime(
                row.get("list_date"),
                errors="coerce",
            )

            delist_date = pd.to_datetime(
                row.get("delist_date"),
                errors="coerce",
            )

            year_end = pd.Timestamp(
                f"{year}-12-31"
            )

            year_start = pd.Timestamp(
                f"{year}-01-01"
            )

            active = True

            if pd.notna(list_date):
                if list_date > year_end:
                    active = False

            if pd.notna(delist_date):
                if delist_date < year_start:
                    active = False

            if active:
                codes.add(code)

                if code not in name_map:
                    name_map[code] = str(
                        row.get("name") or ""
                    )

                if code not in st_map:
                    st_map[code] = is_st_name(
                        row.get("name")
                    )

    codes = sorted(codes)

    # Only request stocks not already checkpointed.
    pending = [
        c for c in codes
        if c not in done
        and c not in empty
    ]

    print(
        f"year={year} total={len(codes)} "
        f"already_done={len(done)} "
        f"already_empty={len(empty)} "
        f"pending={len(pending)}"
    )

    total_success = 0
    total_empty = 0
    total_failed = 0

    for offset in range(
        0,
        len(pending),
        BATCH_SIZE,
    ):

        batch = pending[
            offset:
            offset + BATCH_SIZE
        ]

        print("")
        print(
            f"batch "
            f"{offset + 1}-"
            f"{offset + len(batch)} "
            f"/ {len(pending)}"
        )

        (
            new_df,
            success,
            empty_now,
            failed,
        ) = fetch_batch(
            batch,
            y_start,
            y_end,
        )

        # Save data FIRST.
        if (
            new_df is not None
            and not new_df.empty
        ):

            upsert_year(
                year,
                new_df,
                st_map,
                name_map,
            )

        # Update checkpoint.
        done.update(success)
        empty.update(empty_now)

        # A failed code remains pending for next run.
        failed_old.update(failed)

        # If a previously failed stock succeeds now,
        # remove it from failed.
        failed_old.difference_update(
            success
        )

        failed_old.difference_update(
            empty_now
        )

        state["done"] = sorted(done)
        state["empty"] = sorted(empty)
        state["failed"] = sorted(failed_old)

        state["last_batch"] = (
            offset + len(batch)
        )

        save_year_state(
            year,
            state,
        )

        total_success += len(success)
        total_empty += len(empty_now)
        total_failed += len(failed)

        print(
            f"checkpoint saved: "
            f"success={len(success)} "
            f"empty={len(empty_now)} "
            f"failed={len(failed)}"
        )

    # Determine completion.
    remaining_failed = (
        set(codes)
        - done
        - empty
    )

    complete = len(remaining_failed) == 0

    state["complete"] = complete
    state["failed"] = sorted(
        remaining_failed
    )

    save_year_state(
        year,
        state,
    )

    # Update meta.
    p = year_path(year)

    if os.path.exists(p):

        df = pd.read_parquet(p)

        if not df.empty:

            meta["years"][str(year)] = {
                "complete": complete,
                "rows": int(len(df)),
                "stocks": int(
                    df["code"].nunique()
                ),
                "date_min": str(
                    pd.to_datetime(
                        df["date"]
                    ).min().date()
                ),
                "date_max": str(
                    pd.to_datetime(
                        df["date"]
                    ).max().date()
                ),
                "failed": int(
                    len(remaining_failed)
                ),
            }

    save_meta(meta)

    print("")
    print("=" * 70)

    if complete:
        print(
            f"YEAR {year} COMPLETE"
        )
    else:
        print(
            f"YEAR {year} INCOMPLETE"
        )
        print(
            f"remaining failed: "
            f"{len(remaining_failed)}"
        )

    print("=" * 70)

    return complete


# ============================================================
# Incremental daily sync
# ============================================================

def get_global_date_max(meta):
    dates = []

    for info in meta.get(
        "years",
        {},
    ).values():

        if info.get("date_max"):
            dates.append(
                pd.to_datetime(
                    info["date_max"]
                ).date()
            )

    if not dates:
        return None

    return max(dates)


def incremental_sync(
    universe,
    st_map,
    name_map,
    meta,
):
    today = dt.date.today()

    date_max = get_global_date_max(
        meta
    )

    attempted = None

    if meta.get("attempted_max"):
        attempted = pd.to_datetime(
            meta["attempted_max"]
        ).date()

    candidates = [
        d
        for d in [
            date_max,
            attempted,
        ]
        if d is not None
    ]

    if candidates:
        start = max(candidates) + dt.timedelta(
            days=1
        )
    else:
        start = dt.date(
            START_YEAR,
            1,
            1,
        )

    if start > today:
        print(
            "incremental: already up to date"
        )
        return

    start_str = start.strftime(
        "%Y%m%d"
    )

    end_str = today.strftime(
        "%Y%m%d"
    )

    print("")
    print("=" * 70)
    print(
        f"INCREMENTAL {start_str} -> {end_str}"
    )
    print("=" * 70)

    codes = sorted(
        universe["code"]
        .astype(str)
        .tolist()
    )

    # Process incrementally in batches.
    all_failed = []
    total_rows = 0

    for offset in range(
        0,
        len(codes),
        BATCH_SIZE,
    ):

        batch = codes[
            offset:
            offset + BATCH_SIZE
        ]

        print(
            f"incremental batch "
            f"{offset + 1}-"
            f"{offset + len(batch)}"
        )

        (
            new_df,
            success,
            empty,
            failed,
        ) = fetch_batch(
            batch,
            start_str,
            end_str,
        )

        all_failed.extend(
            failed
        )

        if (
            new_df is not None
            and not new_df.empty
        ):

            new_df["name"] = (
                new_df["code"]
                .map(name_map)
                .fillna("")
            )

            new_df["date"] = pd.to_datetime(
                new_df["date"]
            )

            for year, group in new_df.groupby(
                new_df["date"].dt.year
            ):

                upsert_year(
                    int(year),
                    group,
                    st_map,
                    name_map,
                )

            total_rows += len(new_df)

    # We only advance attempted_max if no request failed.
    if not all_failed:

        meta["attempted_max"] = end_str

    else:

        print(
            f"WARNING: {len(all_failed)} "
            f"stocks failed; will retry next run"
        )

    meta["last_run"] = (
        dt.datetime.utcnow()
        .replace(microsecond=0)
        .isoformat()
        + "Z"
    )

    save_meta(meta)

    print(
        f"incremental rows added: "
        f"{total_rows}"
    )


# ============================================================
# Refresh global metadata
# ============================================================

def refresh_meta(meta):
    rows = 0
    mins = []
    maxs = []

    for year, info in meta.get(
        "years",
        {},
    ).items():

        p = year_path(
            int(year)
        )

        if not os.path.exists(p):
            continue

        try:
            df = pd.read_parquet(p)

            if df.empty:
                continue

            rows += len(df)

            mins.append(
                pd.to_datetime(
                    df["date"]
                ).min().date()
            )

            maxs.append(
                pd.to_datetime(
                    df["date"]
                ).max().date()
            )

        except Exception as e:

            print(
                f"metadata refresh failed "
                f"for {year}: {e}"
            )

    if mins:
        meta["date_min"] = str(
            min(mins)
        )

    if maxs:
        meta["date_max"] = str(
            max(maxs)
        )

    meta["total_rows"] = int(rows)

    save_meta(meta)


# ============================================================
# Failed report
# ============================================================

def write_failed_report():
    failed = []

    for filename in os.listdir(
        STATE_DIR
    ):

        if not filename.endswith(
            ".json"
        ):
            continue

        p = os.path.join(
            STATE_DIR,
            filename,
        )

        try:
            with open(
                p,
                "r",
                encoding="utf-8",
            ) as f:

                state = json.load(f)

            year = state.get(
                "year",
                filename,
            )

            for code in state.get(
                "failed",
                [],
            ):

                failed.append(
                    f"{year},{code}"
                )

        except Exception:
            continue

    failed = sorted(
        set(failed)
    )

    with open(
        FAILED_TXT,
        "w",
        encoding="utf-8",
    ) as f:

        if failed:
            f.write(
                "\n".join(failed)
            )
        else:
            f.write("(none)\n")


# ============================================================
# Main
# ============================================================

def main():

    print("")
    print("=" * 70)
    print("A-SHARE DAILY DATABASE V3")
    print("=" * 70)

    today = dt.date.today()
    current_year = today.year

    meta = load_meta()

    # --------------------------------------------------------
    # 1. Current universe
    # --------------------------------------------------------

    universe = get_current_universe()

    universe.to_csv(
        UNIVERSE_CSV,
        index=False,
        encoding="utf-8-sig",
    )

    st_map = dict(
        zip(
            universe["code"],
            universe["is_st"],
        )
    )

    name_map = dict(
        zip(
            universe["code"],
            universe["name"],
        )
    )

    # --------------------------------------------------------
    # 2. Delisted
    # --------------------------------------------------------

    delisted = get_delisted_best_effort()

    if not delisted.empty:

        delisted.to_csv(
            DELIST_CSV,
            index=False,
            encoding="utf-8-sig",
        )

        for _, row in delisted.iterrows():

            code = norm_code(
                row.get("code", "")
            )

            if not code:
                continue

            if code not in name_map:
                name_map[code] = str(
                    row.get("name") or ""
                )

            if code not in st_map:
                st_map[code] = is_st_name(
                    row.get("name")
                )

    else:

        if not os.path.exists(
            DELIST_CSV
        ):

            pd.DataFrame(
                columns=[
                    "code",
                    "name",
                    "list_date",
                    "delist_date",
                ]
            ).to_csv(
                DELIST_CSV,
                index=False,
                encoding="utf-8-sig",
            )

    # --------------------------------------------------------
    # 3. Historical backfill
    # --------------------------------------------------------

    next_year = get_next_incomplete_year(
        START_YEAR,
        current_year,
        meta,
    )

    if next_year is not None:

        print("")
        print(
            f"Next historical year: "
            f"{next_year}"
        )

        backfill_one_year(
            next_year,
            universe,
            delisted,
            st_map,
            name_map,
            meta,
        )

    else:

        print(
            "Historical backfill is complete."
        )

        # ----------------------------------------------------
        # 4. Daily incremental sync
        # ----------------------------------------------------

        incremental_sync(
            universe,
            st_map,
            name_map,
            meta,
        )

    # --------------------------------------------------------
    # 5. Metadata
    # --------------------------------------------------------

    refresh_meta(meta)

    write_failed_report()

    meta["last_run"] = (
        dt.datetime.utcnow()
        .replace(microsecond=0)
        .isoformat()
        + "Z"
    )

    save_meta(meta)

    print("")
    print("=" * 70)
    print("SYNC FINISHED")
    print("=" * 70)

    print(
        f"date_min={meta.get('date_min')}"
    )

    print(
        f"date_max={meta.get('date_max')}"
    )

    print(
        f"total_rows={meta.get('total_rows')}"
    )

    print(
        "Failed report:",
        FAILED_TXT,
    )


if __name__ == "__main__":
    main()
