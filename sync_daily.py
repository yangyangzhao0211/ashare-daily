#!/usr/bin/env python3
"""
A-share daily database sync V3.2

Purpose
-------
Build and maintain a full-market A-share daily OHLCV database.

Main features
-------------
1. Universe fallback:
   Eastmoney -> AkShare/Tencent -> existing repository universe.csv

2. Historical backfill:
   2019 -> current year, one year at a time.

3. Checkpoint:
   Every BATCH_SIZE stocks are persisted to data/state/YYYY.json.
   A later run resumes from the previous checkpoint.

4. Robust failure handling:
   Individual stock failures are recorded and do not terminate the run.

5. State compatibility:
   Older/broken state files are automatically normalized.
   In particular:
       failed: []  -> failed: {}
       done: {}    -> done: []

6. Data:
   Unadjusted daily prices.
   Eastmoney pct_chg is retained.
   Approximate limit-up / limit-down flags are generated.

7. Delisted stocks:
   Best-effort historical coverage through AkShare.
"""

import datetime as dt
import json
import os
import random
import re
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import local

import pandas as pd
import requests


# ============================================================
# Configuration
# ============================================================

START_YEAR = 2019

# Lower worker count intentionally.
# We prefer stability over maximum API throughput.
MAX_WORKERS = 4

# Save/checkpoint every N stocks.
BATCH_SIZE = 500

# HTTP timeout per request.
REQUEST_TIMEOUT = 15

# Number of retries for an individual request.
RETRIES = 3

# Eastmoney endpoints.
EM_KLINE_URL = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
EM_LIST_URL = "https://push2.eastmoney.com/api/qt/clist/get"

EM_UT = "fa5fd1943c7b386f172d6893dbfba10b"

# A-share universe:
# Shanghai + Shenzhen + Beijing.
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
}


# ============================================================
# Repository paths
# ============================================================

def find_repo_root():
    """
    Find GitHub repository root.
    """
    d = os.path.dirname(os.path.abspath(__file__))

    for _ in range(8):
        if os.path.isdir(os.path.join(d, ".github")):
            return d

        parent = os.path.dirname(d)

        if parent == d:
            break

        d = parent

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

_thread_local = local()


def get_session():
    """
    Thread-local requests session.
    """
    if not hasattr(_thread_local, "session"):
        _thread_local.session = requests.Session()
        _thread_local.session.headers.update(HEADERS)

    return _thread_local.session


# ============================================================
# Basic utilities
# ============================================================

def normalize_code(value):
    """
    Extract a 6-digit A-share code.
    """
    m = re.search(r"(\d{6})", str(value or ""))

    if m:
        return m.group(1)

    return ""


def is_st(name):
    """
    Determine whether a stock name is ST.
    """
    return bool(re.match(r"^\*?ST", str(name or "")))


def secid(code):
    """
    Eastmoney secid.

    Shanghai / Beijing:
        1.600000

    Shenzhen:
        0.000001
    """
    code = str(code).zfill(6)

    if code.startswith(("6", "9")):
        return "1." + code

    return "0." + code


# ============================================================
# Universe
# ============================================================

def get_universe_eastmoney():
    """
    Get current A-share universe directly from Eastmoney.

    Returns
    -------
    DataFrame
        code / name / is_st
    """

    rows = []

    page_size = 5000

    for page in range(1, 10):

        params = {
            "pn": page,
            "pz": page_size,
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

                time.sleep(
                    0.2 + random.random() * 0.3
                )

                response = get_session().get(
                    EM_LIST_URL,
                    params=params,
                    timeout=REQUEST_TIMEOUT,
                )

                response.raise_for_status()

                payload = response.json()

                data = payload.get("data") or {}

                diff = data.get("diff") or []

                if not diff:

                    return pd.DataFrame(
                        rows,
                        columns=["code", "name", "is_st"],
                    )

                for item in diff:

                    code = normalize_code(
                        item.get("f12")
                    )

                    name = str(
                        item.get("f14") or ""
                    )

                    if code:

                        rows.append(
                            (
                                code,
                                name,
                                is_st(name),
                            )
                        )

                if len(diff) < page_size:

                    df = pd.DataFrame(
                        rows,
                        columns=[
                            "code",
                            "name",
                            "is_st",
                        ],
                    )

                    return (
                        df
                        .drop_duplicates("code")
                        .sort_values("code")
                        .reset_index(drop=True)
                    )

                break

            except Exception as exc:

                last_error = exc

                wait = 2 ** attempt

                print(
                    f"Eastmoney universe attempt "
                    f"{attempt + 1}/{RETRIES} failed: "
                    f"{exc}"
                )

                time.sleep(wait)

        if last_error is not None and page > 1:
            raise last_error

    df = pd.DataFrame(
        rows,
        columns=[
            "code",
            "name",
            "is_st",
        ],
    )

    return (
        df
        .drop_duplicates("code")
        .sort_values("code")
        .reset_index(drop=True)
    )


def get_universe_akshare():
    """
    AkShare fallback.

    Try:
        1. Tencent spot
        2. Eastmoney spot
        3. static A-share code list
    """

    script = r'''
import akshare as ak
import pandas as pd
import re
import json

candidates = [
    "stock_zh_a_spot_tx",
    "stock_zh_a_spot_em",
    "stock_info_a_code_name",
]

for fn in candidates:

    try:

        df = getattr(ak, fn)()

        if df is None or df.empty:
            continue

        if fn == "stock_zh_a_spot_tx":

            code = (
                df["code"]
                .astype(str)
                .str.extract(r"(\d{6})")[0]
            )

            name = df.get(
                "name",
                code
            ).astype(str)

        elif fn == "stock_zh_a_spot_em":

            code = (
                df["代码"]
                .astype(str)
            )

            name = (
                df["名称"]
                .astype(str)
            )

        else:

            code = (
                df["code"]
                .astype(str)
            )

            name = (
                df["name"]
                .astype(str)
            )

        out = pd.DataFrame(
            {
                "code": code,
                "name": name,
            }
        )

        out = out.dropna(
            subset=["code"]
        )

        out["code"] = (
            out["code"]
            .str.extract(r"(\d{6})")[0]
        )

        out["is_st"] = (
            out["name"]
            .map(
                lambda x:
                bool(
                    re.match(
                        r"^\*?ST",
                        str(x)
                    )
                )
            )
        )

        out = (
            out
            .dropna(subset=["code"])
            .drop_duplicates("code")
        )

        # A complete A-share universe should be
        # well above 1000 stocks.
        if len(out) >= 1000:

            print(
                out.to_json(
                    orient="records",
                    force_ascii=False
                )
            )

            break

    except Exception:
        continue
'''

    try:

        result = subprocess.run(
            [
                sys.executable,
                "-c",
                script,
            ],
            capture_output=True,
            text=True,
            timeout=180,
        )

        lines = [
            x
            for x in result.stdout.splitlines()
            if x.strip()
        ]

        if not lines:
            return None

        df = pd.DataFrame(
            json.loads(lines[-1])
        )

        if len(df) >= 1000:

            return df[
                [
                    "code",
                    "name",
                    "is_st",
                ]
            ]

    except Exception as exc:

        print(
            "AkShare universe fallback failed:",
            exc
        )

    return None


def get_universe():

    # --------------------------------------------------------
    # 1. Eastmoney
    # --------------------------------------------------------

    try:

        df = get_universe_eastmoney()

        if len(df) >= 1000:

            print(
                f"universe: Eastmoney "
                f"{len(df)} stocks"
            )

            return df

        print(
            "Eastmoney universe incomplete; "
            "falling back"
        )

    except Exception as exc:

        print(
            "Eastmoney universe failed:",
            exc
        )

    # --------------------------------------------------------
    # 2. AkShare
    # --------------------------------------------------------

    df = get_universe_akshare()

    if df is not None:

        print(
            f"universe: AkShare fallback "
            f"{len(df)} stocks"
        )

        return df

    # --------------------------------------------------------
    # 3. Existing repository universe
    # --------------------------------------------------------

    if os.path.exists(UNIVERSE_CSV):

        try:

            df = pd.read_csv(
                UNIVERSE_CSV,
                dtype={"code": str},
            )

            df["code"] = (
                df["code"]
                .map(normalize_code)
            )

            df = (
                df
                .dropna(subset=["code"])
                .drop_duplicates("code")
            )

            if len(df) >= 1000:

                print(
                    f"universe: repository fallback "
                    f"{len(df)} stocks"
                )

                return df[
                    [
                        "code",
                        "name",
                        "is_st",
                    ]
                ]

            print(
                "Repository universe exists but "
                f"contains only {len(df)} stocks; "
                "refusing to use partial universe."
            )

        except Exception as exc:

            print(
                "Repository universe fallback failed:",
                exc
            )

    raise RuntimeError(
        "Cannot obtain a reliable A-share universe."
    )


# ============================================================
# Delisted stocks
# ============================================================

def get_delisted():

    script = r'''
import akshare as ak
import pandas as pd
import json

frames = []

# Shanghai
try:

    sh = ak.stock_info_sh_delist("全部")

    sh = sh.rename(
        columns={
            "公司代码": "code",
            "公司简称": "name",
            "上市日期": "list_date",
            "暂停上市日期": "delist_date",
        }
    )

    keep = [
        c for c in
        [
            "code",
            "name",
            "list_date",
            "delist_date",
        ]
        if c in sh.columns
    ]

    frames.append(
        sh[keep]
    )

except Exception:
    pass


# Shenzhen
try:

    sz = ak.stock_info_sz_delist(
        "终止上市公司"
    )

    rename = {}

    for c in sz.columns:

        s = str(c)

        if "代码" in s:
            rename[c] = "code"

        elif (
            "简称" in s
            or "名称" in s
        ):
            rename[c] = "name"

        elif "上市日期" in s:
            rename[c] = "list_date"

        elif (
            "终止" in s
            and "日期" in s
        ):
            rename[c] = "delist_date"

    sz = sz.rename(
        columns=rename
    )

    keep = [
        c for c in
        [
            "code",
            "name",
            "list_date",
            "delist_date",
        ]
        if c in sz.columns
    ]

    frames.append(
        sz[keep]
    )

except Exception:
    pass


if frames:

    df = pd.concat(
        frames,
        ignore_index=True
    )

    for c in [
        "code",
        "name",
        "list_date",
        "delist_date",
    ]:

        if c not in df.columns:
            df[c] = None

    df["code"] = (
        df["code"]
        .astype(str)
        .str.extract(r"(\d{6})")[0]
    )

    df = (
        df
        .dropna(subset=["code"])
        .drop_duplicates("code")
    )

    for c in [
        "list_date",
        "delist_date",
    ]:

        df[c] = pd.to_datetime(
            df[c],
            errors="coerce"
        )

    print(
        df.to_json(
            orient="records",
            force_ascii=False
        )
    )

else:

    print("[]")
'''

    try:

        result = subprocess.run(
            [
                sys.executable,
                "-c",
                script,
            ],
            capture_output=True,
            text=True,
            timeout=180,
        )

        lines = [
            x
            for x in result.stdout.splitlines()
            if x.strip()
        ]

        if lines:

            return pd.DataFrame(
                json.loads(lines[-1])
            )

    except Exception as exc:

        print(
            "Delisted lookup failed; "
            "continuing:",
            exc
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
# Historical data
# ============================================================

def fetch_one(
    code,
    start,
    end,
):
    """
    Fetch one stock's unadjusted daily bars.
    """

    params = {
        "secid": secid(code),
        "ut": EM_UT,
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": (
            "f51,f52,f53,f54,f55,"
            "f56,f57,f58,f59,f60,f61"
        ),
        "klt": 101,
        "fqt": 0,
        "beg": start,
        "end": end,
    }

    last_error = ""

    for attempt in range(RETRIES):

        try:

            time.sleep(
                0.15 + random.random() * 0.25
            )

            response = get_session().get(
                EM_KLINE_URL,
                params=params,
                timeout=REQUEST_TIMEOUT,
            )

            response.raise_for_status()

            payload = response.json()

            data = payload.get("data") or {}

            klines = data.get("klines") or []

            # No trading records in this year.
            if not klines:

                return "EMPTY", None

            rows = []

            for line in klines:

                parts = str(line).split(",")

                if len(parts) >= 11:

                    rows.append(
                        [
                            parts[0],   # date
                            code,
                            parts[1],   # open
                            parts[2],   # close
                            parts[3],   # high
                            parts[4],   # low
                            parts[5],   # volume
                            parts[6],   # amount
                            parts[10],  # turnover
                            parts[8],   # pct_chg
                        ]
                    )

            if not rows:

                return "EMPTY", None

            df = pd.DataFrame(
                rows,
                columns=[
                    "date",
                    "code",
                    "open",
                    "close",
                    "high",
                    "low",
                    "volume",
                    "amount",
                    "turnover",
                    "pct_chg",
                ],
            )

            df["date"] = pd.to_datetime(
                df["date"],
                errors="coerce",
            )

            numeric_cols = [
                "open",
                "close",
                "high",
                "low",
                "volume",
                "amount",
                "turnover",
                "pct_chg",
            ]

            for col in numeric_cols:

                df[col] = pd.to_numeric(
                    df[col],
                    errors="coerce",
                )

            df["src"] = "em"

            return "OK", df

        except Exception as exc:

            last_error = str(exc)

            wait = (
                1.5 * (2 ** attempt)
                + random.random()
            )

            time.sleep(wait)

    return "FAILED", last_error


# ============================================================
# State / checkpoint
# ============================================================

def state_path(year):

    return os.path.join(
        STATE_DIR,
        f"{year}.json",
    )


def normalize_state(year, state):
    """
    Normalize old or malformed state files.

    This is the important V3.2 fix.

    Old state might contain:
        "failed": []

    New format requires:
        "failed": {}
    """

    if not isinstance(state, dict):

        state = {}

    state["year"] = year

    # --------------------------------------------------------
    # done
    # --------------------------------------------------------

    done = state.get("done", [])

    if isinstance(done, dict):

        done = list(done.keys())

    elif isinstance(done, set):

        done = list(done)

    elif not isinstance(done, list):

        done = []

    normalized_done = []

    for code in done:

        code = normalize_code(code)

        if code:
            normalized_done.append(code)

    state["done"] = sorted(
        set(normalized_done)
    )

    # --------------------------------------------------------
    # failed
    # --------------------------------------------------------

    failed = state.get("failed", {})

    # V3.1 bug:
    # failed could have been saved as a list.
    if isinstance(failed, list):

        converted = {}

        for code in failed:

            code = normalize_code(code)

            if code:
                converted[code] = "previous failure"

        failed = converted

    elif not isinstance(failed, dict):

        failed = {}

    normalized_failed = {}

    for code, message in failed.items():

        code = normalize_code(code)

        if code:

            normalized_failed[code] = str(
                message
            )

    state["failed"] = normalized_failed

    # --------------------------------------------------------
    # complete
    # --------------------------------------------------------

    state["complete"] = bool(
        state.get("complete", False)
    )

    return state


def load_state(year):

    path = state_path(year)

    if not os.path.exists(path):

        return {
            "year": year,
            "done": [],
            "failed": {},
            "complete": False,
        }

    try:

        with open(
            path,
            "r",
            encoding="utf-8",
        ) as f:

            raw = json.load(f)

        state = normalize_state(
            year,
            raw,
        )

        # Immediately rewrite normalized state.
        save_state(
            year,
            state,
        )

        return state

    except Exception as exc:

        print(
            f"WARNING: state file for "
            f"{year} is unreadable: {exc}"
        )

        return {
            "year": year,
            "done": [],
            "failed": {},
            "complete": False,
        }


def save_state(
    year,
    state,
):

    state = normalize_state(
        year,
        state,
    )

    path = state_path(year)

    tmp = path + ".tmp"

    with open(
        tmp,
        "w",
        encoding="utf-8",
    ) as f:

        json.dump(
            state,
            f,
            ensure_ascii=False,
            indent=2,
        )

    os.replace(
        tmp,
        path,
    )


# ============================================================
# Limit-up / limit-down flags
# ============================================================

def limit_threshold(
    code,
    st,
    date,
):

    d = pd.Timestamp(date).date()

    if st:
        return 5.0

    # STAR
    if code.startswith("688"):
        return 20.0

    # ChiNext
    if code[:3] in (
        "300",
        "301",
    ):

        if d >= dt.date(
            2020,
            8,
            24,
        ):

            return 20.0

        return 10.0

    # Beijing Stock Exchange
    if code.startswith("8"):

        if d >= dt.date(
            2021,
            11,
            15,
        ):

            return 30.0

        return 10.0

    # Main board
    return 10.0


def add_limit_flags(
    df,
    st_map,
):

    df = df.copy()

    df["is_st"] = (
        df["code"]
        .map(st_map)
        .fillna(False)
    )

    thresholds = pd.Series(
        [
            limit_threshold(
                code,
                st,
                date,
            )
            for code, st, date
            in zip(
                df["code"],
                df["is_st"],
                df["date"],
            )
        ],
        index=df.index,
    )

    # Research approximation.
    # Not an official exchange flag.
    df["limit_up_approx"] = (
        df["pct_chg"]
        >= thresholds - 0.5
    )

    df["limit_down_approx"] = (
        df["pct_chg"]
        <= -(
            thresholds - 0.5
        )
    )

    return df.drop(
        columns=["is_st"]
    )


# ============================================================
# Parquet merge
# ============================================================

def merge_year(
    year,
    new,
    st_map,
):

    if new is None or new.empty:
        return

    path = os.path.join(
        DAILY_DIR,
        f"{year}.parquet",
    )

    new = add_limit_flags(
        new,
        st_map,
    )

    if os.path.exists(path):

        old = pd.read_parquet(path)

        df = pd.concat(
            [
                old,
                new,
            ],
            ignore_index=True,
        )

    else:

        df = new

    # Critical deduplication.
    df = (
        df
        .drop_duplicates(
            subset=[
                "date",
                "code",
            ],
            keep="last",
        )
        .sort_values(
            [
                "date",
                "code",
            ]
        )
        .reset_index(
            drop=True
        )
    )

    tmp = path + ".tmp"

    df.to_parquet(
        tmp,
        index=False,
    )

    os.replace(
        tmp,
        path,
    )


# ============================================================
# Git checkpoint
# ============================================================

def git_checkpoint(
    label,
):

    try:

        subprocess.run(
            [
                "git",
                "config",
                "user.name",
                "ashare-bot",
            ],
            check=True,
        )

        subprocess.run(
            [
                "git",
                "config",
                "user.email",
                "ashare-bot@users.noreply.github.com",
            ],
            check=True,
        )

        subprocess.run(
            [
                "git",
                "add",
                "data/",
            ],
            check=True,
        )

        check = subprocess.run(
            [
                "git",
                "diff",
                "--cached",
                "--quiet",
            ]
        )

        if check.returncode == 0:

            return

        subprocess.run(
            [
                "git",
                "commit",
                "-m",
                label,
            ],
            check=True,
        )

        subprocess.run(
            [
                "git",
                "push",
            ],
            check=True,
        )

        print(
            "checkpoint pushed:",
            label,
        )

    except Exception as exc:

        # Important:
        # Failure to push a checkpoint does NOT kill
        # the current Python process.
        print(
            "WARNING: checkpoint push failed; "
            "continuing:",
            exc,
        )


# ============================================================
# Process one historical year
# ============================================================

def process_year(
    year,
    universe,
    delisted,
    st_map,
    name_map,
):

    state = load_state(year)

    done = set(
        state.get(
            "done",
            [],
        )
    )

    # --------------------------------------------------------
    # Build active universe for this historical year.
    # --------------------------------------------------------

    active = list(
        universe["code"]
        .astype(str)
    )

    if (
        delisted is not None
        and not delisted.empty
    ):

        for _, row in delisted.iterrows():

            try:

                code = normalize_code(
                    row.get("code")
                )

                if not code:
                    continue

                list_date = row.get(
                    "list_date"
                )

                delist_date = row.get(
                    "delist_date"
                )

                list_ok = (
                    pd.isna(list_date)
                    or pd.Timestamp(
                        list_date
                    )
                    <= pd.Timestamp(
                        f"{year}-12-31"
                    )
                )

                delist_ok = (
                    pd.isna(delist_date)
                    or pd.Timestamp(
                        delist_date
                    )
                    >= pd.Timestamp(
                        f"{year}-01-01"
                    )
                )

                if list_ok and delist_ok:

                    active.append(code)

            except Exception:
                continue

    active = sorted(
        set(active)
    )

    remaining = [
        code
        for code in active
        if code not in done
    ]

    print(
        f"{year}: total={len(active)} "
        f"remaining={len(remaining)}"
    )

    # --------------------------------------------------------
    # Already complete.
    # --------------------------------------------------------

    if not remaining:

        state["complete"] = True

        state["done"] = sorted(
            set(active)
        )

        state["failed"] = {}

        save_state(
            year,
            state,
        )

        return True

    start = f"{year}0101"
    end = f"{year}1231"

    # --------------------------------------------------------
    # Batch processing.
    # --------------------------------------------------------

    for offset in range(
        0,
        len(remaining),
        BATCH_SIZE,
    ):

        batch = remaining[
            offset:
            offset + BATCH_SIZE
        ]

        print(
            f"{year}: processing batch "
            f"{offset + 1}-"
            f"{offset + len(batch)} "
            f"/ {len(remaining)}"
        )

        batch_failed = []

        frames = []

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
                for code in batch
            }

            for index, future in enumerate(
                as_completed(futures),
                1,
            ):

                code = futures[
                    future
                ]

                try:

                    status, value = (
                        future.result()
                    )

                except Exception as exc:

                    status = "FAILED"

                    value = str(exc)

                # ------------------------------------------------
                # Successful stock
                # ------------------------------------------------

                if status == "OK":

                    frames.append(
                        value
                    )

                    done.add(code)

                    # If it previously failed,
                    # remove old failure record.
                    state["failed"].pop(
                        code,
                        None,
                    )

                # ------------------------------------------------
                # No data for this year.
                # ------------------------------------------------

                elif status == "EMPTY":

                    # Empty is a valid terminal state.
                    done.add(code)

                    state["failed"].pop(
                        code,
                        None,
                    )

                # ------------------------------------------------
                # Failed request.
                # ------------------------------------------------

                elif status == "FAILED":

                    batch_failed.append(
                        code
                    )

                    state["failed"][
                        code
                    ] = str(value)

                if index % 100 == 0:

                    print(
                        f"  {year}: "
                        f"{offset + index}/"
                        f"{len(remaining)}"
                    )

        # --------------------------------------------------------
        # Merge successful records.
        # --------------------------------------------------------

        if frames:

            new = pd.concat(
                frames,
                ignore_index=True,
            )

            new["name"] = (
                new["code"]
                .map(name_map)
                .fillna(new["code"])
            )

            merge_year(
                year,
                new,
                st_map,
            )

        # --------------------------------------------------------
        # Failed stocks are NOT done.
        # --------------------------------------------------------

        for code in batch_failed:

            done.discard(code)

        # --------------------------------------------------------
        # Save checkpoint.
        # --------------------------------------------------------

        state["done"] = sorted(
            set(done)
        )

        state["complete"] = False

        state["last_batch_start"] = (
            offset + 1
        )

        state["last_batch_end"] = (
            min(
                offset + len(batch),
                len(remaining),
            )
        )

        state["total_remaining_at_start"] = (
            len(remaining)
        )

        state["updated_at"] = (
            dt.datetime.utcnow()
            .replace(microsecond=0)
            .isoformat()
            + "Z"
        )

        save_state(
            year,
            state,
        )

        # --------------------------------------------------------
        # Push checkpoint to GitHub.
        # --------------------------------------------------------

        git_checkpoint(
            f"checkpoint {year} "
            f"{state['last_batch_end']}/"
            f"{len(remaining)}"
        )

        print(
            f"{year}: checkpoint saved. "
            f"done={len(done)} "
            f"failed={len(state['failed'])}"
        )

    # --------------------------------------------------------
    # Year completed?
    # --------------------------------------------------------

    state["done"] = sorted(
        set(done)
    )

    unresolved = [
        code
        for code in active
        if code not in done
    ]

    state["complete"] = (
        len(unresolved) == 0
    )

    state["failed_count"] = len(
        unresolved
    )

    state["updated_at"] = (
        dt.datetime.utcnow()
        .replace(microsecond=0)
        .isoformat()
        + "Z"
    )

    save_state(
        year,
        state,
    )

    if state["complete"]:

        git_checkpoint(
            f"complete {year}"
        )

        print(
            f"{year}: COMPLETE"
        )

        return True

    print(
        f"{year}: incomplete; "
        f"unresolved={len(unresolved)}"
    )

    return False


# ============================================================
# Meta
# ============================================================

def load_meta():

    if not os.path.exists(
        META_JSON
    ):

        return {
            "date_max": None,
            "date_min": None,
            "total_rows": 0,
        }

    try:

        with open(
            META_JSON,
            "r",
            encoding="utf-8",
        ) as f:

            meta = json.load(f)

        if not isinstance(
            meta,
            dict,
        ):

            return {}

        return meta

    except Exception:

        return {}


def save_meta(meta):

    tmp = META_JSON + ".tmp"

    with open(
        tmp,
        "w",
        encoding="utf-8",
    ) as f:

        json.dump(
            meta,
            f,
            ensure_ascii=False,
            indent=2,
        )

    os.replace(
        tmp,
        META_JSON,
    )


# ============================================================
# Incremental daily sync
# ============================================================

def run_incremental(
    today,
    universe,
    st_map,
    name_map,
):

    meta = load_meta()

    last = None

    raw_last = meta.get(
        "date_max"
    )

    if raw_last:

        try:

            last = pd.to_datetime(
                raw_last
            ).date()

        except Exception:

            last = None

    # Safety fallback.
    if last is None:

        last = (
            today
            - dt.timedelta(days=7)
        )

    if last >= today:

        print(
            "incremental: "
            "already up to date"
        )

        return

    start_date = (
        last
        + dt.timedelta(days=1)
    )

    end_date = today

    start = start_date.strftime(
        "%Y%m%d"
    )

    end = end_date.strftime(
        "%Y%m%d"
    )

    print(
        f"incremental: "
        f"{start} -> {end}"
    )

    frames = []

    failed = []

    codes = sorted(
        universe["code"]
        .astype(str)
    )

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

        for future in as_completed(
            futures
        ):

            code = futures[
                future
            ]

            try:

                status, value = (
                    future.result()
                )

            except Exception:

                status = "FAILED"

                value = None

            if status == "OK":

                frames.append(
                    value
                )

            elif status == "FAILED":

                failed.append(
                    code
                )

    if frames:

        new = pd.concat(
            frames,
            ignore_index=True,
        )

        new["name"] = (
            new["code"]
            .map(name_map)
            .fillna(new["code"])
        )

        for year, group in (
            new.groupby(
                new["date"].dt.year
            )
        ):

            merge_year(
                int(year),
                group,
                st_map,
            )

        actual_date_max = (
            new["date"]
            .max()
            .date()
        )

        meta["date_max"] = str(
            actual_date_max
        )

    else:

        # Do NOT falsely advance date_max
        # if every request failed.
        if len(failed) == 0:

            # No stock had data. This is likely
            # a weekend/holiday.
            meta["date_max"] = str(
                last
            )

        else:

            print(
                f"WARNING: "
                f"{len(failed)} stocks failed; "
                "date_max will not advance."
            )

    # Global row count.
    total_rows = 0

    date_min = None
    date_max = None

    for filename in os.listdir(
        DAILY_DIR
    ):

        if not filename.endswith(
            ".parquet"
        ):
            continue

        path = os.path.join(
            DAILY_DIR,
            filename,
        )

        try:

            df = pd.read_parquet(
                path,
                columns=[
                    "date",
                    "code",
                ],
            )

            total_rows += len(df)

            if not df.empty:

                local_min = pd.to_datetime(
                    df["date"]
                ).min()

                local_max = pd.to_datetime(
                    df["date"]
                ).max()

                if (
                    date_min is None
                    or local_min < date_min
                ):

                    date_min = local_min

                if (
                    date_max is None
                    or local_max > date_max
                ):

                    date_max = local_max

        except Exception as exc:

            print(
                f"WARNING: cannot inspect "
                f"{filename}: {exc}"
            )

    meta["total_rows"] = int(
        total_rows
    )

    if date_min is not None:

        meta["date_min"] = str(
            date_min.date()
        )

    if date_max is not None:

        meta["date_max"] = str(
            date_max.date()
        )

    meta["updated_at"] = (
        dt.datetime.utcnow()
        .replace(microsecond=0)
        .isoformat()
        + "Z"
    )

    save_meta(
        meta
    )

    with open(
        FAILED_TXT,
        "w",
        encoding="utf-8",
    ) as f:

        f.write(
            "\n".join(
                sorted(
                    set(failed)
                )
            )
            or "(none)"
        )

    git_checkpoint(
        f"daily sync "
        f"{today.isoformat()}"
    )


# ============================================================
# Main
# ============================================================

def main():

    today = dt.date.today()

    print("=" * 70)

    print(
        "A-share daily database sync V3.2"
    )

    print(
        f"Date: {today}"
    )

    print(
        f"START_YEAR: {START_YEAR}"
    )

    print(
        f"MAX_WORKERS: {MAX_WORKERS}"
    )

    print(
        f"BATCH_SIZE: {BATCH_SIZE}"
    )

    print("=" * 70)

    # --------------------------------------------------------
    # 1. Universe
    # --------------------------------------------------------

    universe = get_universe()

    universe["code"] = (
        universe["code"]
        .map(normalize_code)
    )

    universe = (
        universe
        .dropna(subset=["code"])
        .drop_duplicates("code")
        .sort_values("code")
        .reset_index(drop=True)
    )

    universe.to_csv(
        UNIVERSE_CSV,
        index=False,
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

    print(
        f"Current universe: "
        f"{len(universe)} stocks"
    )

    # --------------------------------------------------------
    # 2. Delisted
    # --------------------------------------------------------

    delisted = get_delisted()

    if delisted is None:

        delisted = pd.DataFrame(
            columns=[
                "code",
                "name",
                "list_date",
                "delist_date",
            ]
        )

    delisted.to_csv(
        DELIST_CSV,
        index=False,
    )

    for _, row in delisted.iterrows():

        code = normalize_code(
            row.get("code")
        )

        if not code:
            continue

        st_map.setdefault(
            code,
            is_st(
                row.get(
                    "name",
                    "",
                )
            ),
        )

        name_map.setdefault(
            code,
            row.get(
                "name",
                "",
            ),
        )

    print(
        f"Delisted records: "
        f"{len(delisted)}"
    )

    # --------------------------------------------------------
    # 3. Historical backfill
    # --------------------------------------------------------

    for year in range(
        START_YEAR,
        today.year + 1,
    ):

        print()
        print(
            "=" * 70
        )

        print(
            f"PROCESS YEAR {year}"
        )

        print(
            "=" * 70
        )

        completed = process_year(
            year,
            universe,
            delisted,
            st_map,
            name_map,
        )

        if not completed:

            print()
            print(
                f"{year} is not complete."
            )

            print(
                "Stopping historical "
                "backfill here."
            )

            print(
                "The next GitHub Actions run "
                "will resume from checkpoint."
            )

            return

    # --------------------------------------------------------
    # 4. Verify all historical years
    # --------------------------------------------------------

    all_complete = True

    for year in range(
        START_YEAR,
        today.year + 1,
    ):

        state = load_state(
            year
        )

        if not state.get(
            "complete",
            False,
        ):

            all_complete = False

            print(
                f"WARNING: {year} "
                "not complete."
            )

    # --------------------------------------------------------
    # 5. Incremental sync
    # --------------------------------------------------------

    if all_complete:

        print()
        print(
            "=" * 70
        )

        print(
            "ALL HISTORICAL YEARS COMPLETE"
        )

        print(
            "Starting incremental sync"
        )

        print(
            "=" * 70
        )

        run_incremental(
            today,
            universe,
            st_map,
            name_map,
        )

    else:

        print(
            "Historical backfill incomplete; "
            "incremental sync deferred."
        )


# ============================================================
# Entry point
# ============================================================

if __name__ == "__main__":
    main()
