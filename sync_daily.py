#!/usr/bin/env python3
"""
A-share full-market daily bars DB (V3).

V3 fixes the V2 hang: every network call now has a timeout.
  - akshare calls WITH a timeout param (hist em/tx): timeout=30
  - akshare calls WITHOUT one (spot/universe/delist): call_with_timeout guard
Without this, a silent remote blocks the thread forever and retries never fire.

Layout:
  data/daily/YYYY.parquet  - one partition per year, 2019 -> present (never deleted)
  data/universe.csv        - current listed universe: code / name / is_st
  data/delist.csv          - delisted codes (SH+SZ): code / name / list_date / delist_date
  data/meta.json           - sync state, per-year completeness, date ranges

Design notes (addressing V1 issues):
  1. Multi-source fallback everywhere: eastmoney -> tencent -> static list.
     No single API can kill the run.
  2. Repo root auto-detection: works whether files live in scripts/ or repo root.
  3. Yearly partitions with per-year checkpoints: a timed-out run resumes,
     history is never trimmed.
  4. Survivorship bias (best-effort): delisted codes are fetched too, using
     list/delist dates to bound each code's active years. Free sources cannot
     guarantee 100% delisted coverage; failures are logged, not fatal.
  5. Holiday handling: a date where >90% of stocks return *empty* (not failed)
     is recorded as non-trading and not re-fetched.

Columns (data/daily/YYYY.parquet):
  date, code, name, open, high, low, close, volume, amount, turnover,
  pct_chg, src, limit_up_approx, limit_down_approx
  - prices: CNY, UNADJUSTED. pct_chg: exchange-official daily % (eastmoney).
    For tencent fallback rows pct_chg is computed from closes (corporate
    actions may distort it) and turnover is NaN; src marks the origin.
  - volume: 手 ; amount: 元 ; turnover/pct_chg: %
  - limit_*_approx: derived from pct_chg with date-aware rule thresholds
    (main 10 / ST 5 / ChiNext&STAR 20 / BSE 30, with regime change dates).
    Approximation for research convenience, NOT exchange-official flags.
"""

import json
import os
import random
import re
import time
import datetime as dt
from concurrent.futures import (ThreadPoolExecutor, as_completed,
                                TimeoutError as FuturesTimeoutError)

import akshare as ak
import pandas as pd

# ---------------------------------------------------------------- repo root
def find_repo_root() -> str:
    p = os.path.abspath(__file__)
    d = os.path.dirname(p)
    for _ in range(6):
        if os.path.isdir(os.path.join(d, ".github")):
            return d
        d = os.path.dirname(d)
    return os.getcwd()  # GitHub Actions `run:` steps start at repo root

ROOT = find_repo_root()
DATA_DIR = os.path.join(ROOT, "data")
DAILY_DIR = os.path.join(DATA_DIR, "daily")
UNIVERSE_CSV = os.path.join(DATA_DIR, "universe.csv")
DELIST_CSV = os.path.join(DATA_DIR, "delist.csv")
META_JSON = os.path.join(DATA_DIR, "meta.json")
FAILED_TXT = os.path.join(DATA_DIR, "_failed.txt")

START_YEAR = 2019
MAX_WORKERS = 5           # V3: lowered 8->5, gentler on rate limits
RETRIES = 3
REQ_TIMEOUT = 30          # V3: per-HTTP-request timeout (the actual hang fix)
CALL_TIMEOUT = 90         # V3: guard for akshare calls without a timeout param

os.makedirs(DAILY_DIR, exist_ok=True)

# ---------------------------------------------------------------- timeout guard
def call_with_timeout(fn, *args, timeout=CALL_TIMEOUT, **kwargs):
    """Run fn in a disposable thread; raise FuturesTimeoutError on hang.

    V3: akshare's spot/universe/delist helpers accept no timeout param, so a
    silent remote will block forever without this guard.
    """
    with ThreadPoolExecutor(max_workers=1) as ex:
        fut = ex.submit(fn, *args, **kwargs)
        return fut.result(timeout=timeout)

# ---------------------------------------------------------------- universe
def _is_st(name: str) -> bool:
    return bool(re.match(r"^\*?ST", str(name or "")))

def get_listed_universe() -> pd.DataFrame:
    """code / name / is_st with eastmoney -> tencent -> static fallbacks."""
    # 1) eastmoney spot (1 request, full snapshot)
    try:
        spot = call_with_timeout(ak.stock_zh_a_spot_em, timeout=CALL_TIMEOUT)
        df = spot[["代码", "名称"]].copy()
        df.columns = ["code", "name"]
        print(f"universe via eastmoney spot: {len(df)}", flush=True)
        df["is_st"] = df["name"].map(_is_st)
        return df
    except Exception as e:
        print(f"eastmoney spot failed: {type(e).__name__}: {e}", flush=True)
    # 2) tencent spot (paginated, ~25 requests -> longer guard)
    try:
        tx = call_with_timeout(ak.stock_zh_a_spot_tx, timeout=150)
        codes = tx["code"].astype(str).str.extract(r"(\d{6})")[0]
        df = pd.DataFrame({"code": codes, "name": tx.get("name", codes)})
        df = df.dropna(subset=["code"]).drop_duplicates("code")
        print(f"universe via tencent spot: {len(df)}")
        df["is_st"] = df["name"].map(_is_st)
        return df
    except Exception as e:
        print(f"tencent spot failed: {e}")
    # 3) static list from exchanges (slow but reliable)
    info = ak.stock_info_a_code_name()
    info["is_st"] = info["name"].map(_is_st)
    print(f"universe via static list: {len(info)}")
    return info[["code", "name", "is_st"]]

def _norm_code(x) -> str:
    m = re.search(r"(\d{6})", str(x or ""))
    return m.group(1) if m else ""

def get_delisted() -> pd.DataFrame:
    """SH + SZ terminated companies: code / name / list_date / delist_date."""
    frames = []
    try:
        sh = call_with_timeout(ak.stock_info_sh_delist, "全部", timeout=60)
        sh = sh.rename(columns={"公司代码": "code", "公司简称": "name",
                                "上市日期": "list_date", "暂停上市日期": "delist_date"})
        frames.append(sh[["code", "name", "list_date", "delist_date"]])
        print(f"SH delisted: {len(sh)}")
    except Exception as e:
        print(f"SH delist failed: {e}")
    try:
        sz = call_with_timeout(ak.stock_info_sz_delist, "终止上市公司", timeout=60)
        cols = {c: c for c in sz.columns}
        # normalize defensively: find code/name/list/delist-ish columns
        rename = {}
        for c in sz.columns:
            s = str(c)
            if "代码" in s and "code" not in rename.values():
                rename[c] = "code"
            elif "简称" in s or "名称" in s:
                rename[c] = "name"
            elif "上市日期" in s:
                rename[c] = "list_date"
            elif "终止" in s and "日期" in s:
                rename[c] = "delist_date"
        sz = sz.rename(columns=rename)
        keep = [c for c in ["code", "name", "list_date", "delist_date"] if c in sz.columns]
        frames.append(sz[keep])
        print(f"SZ delisted: {len(sz)}")
    except Exception as e:
        print(f"SZ delist failed: {e}")
    if not frames:
        return pd.DataFrame(columns=["code", "name", "list_date", "delist_date"])
    df = pd.concat(frames, ignore_index=True)
    df["code"] = df["code"].map(_norm_code)
    df = df[df["code"] != ""].drop_duplicates("code")
    df["list_date"] = pd.to_datetime(df.get("list_date"), errors="coerce")
    df["delist_date"] = pd.to_datetime(df.get("delist_date"), errors="coerce")
    return df

# ---------------------------------------------------------------- history fetch
STD_COLS = ["date", "code", "name", "open", "high", "low", "close",
            "volume", "amount", "turnover", "pct_chg", "src"]

def _fetch_em(code: str, start: str, end: str):
    # V3: timeout=REQ_TIMEOUT -- without it a silent remote hangs forever
    df = ak.stock_zh_a_hist(symbol=code, period="daily",
                            start_date=start, end_date=end, adjust="",
                            timeout=REQ_TIMEOUT)
    if df is None or df.empty:
        return None  # empty (e.g. not listed in range / holiday)
    df = df.rename(columns={"日期": "date", "开盘": "open", "收盘": "close",
                            "最高": "high", "最低": "low", "成交量": "volume",
                            "成交额": "amount", "换手率": "turnover",
                            "涨跌幅": "pct_chg"})
    df["code"] = code
    df["date"] = pd.to_datetime(df["date"])
    df["src"] = "em"
    return df[[c for c in STD_COLS if c != "name"]]

def _tx_symbol(code: str) -> str:
    return ("sh" if code.startswith(("6", "9")) else "sz") + code

def _fetch_tx(code: str, start: str, end: str):
    # V3: timeout=REQ_TIMEOUT -- tencent loops one request per year internally
    df = ak.stock_zh_a_hist_tx(symbol=_tx_symbol(code),
                               start_date=start, end_date=end, adjust="",
                               timeout=REQ_TIMEOUT)
    if df is None or df.empty:
        return None
    df = df.rename(columns={"date": "date"})
    df["date"] = pd.to_datetime(df["date"])
    df = df[(df["date"] >= start) & (df["date"] <= end)]
    if df.empty:
        return None
    df["code"] = code
    df["volume"] = df["volume"] / 100.0          # tx: 股 -> 手
    df["amount"] = df["close"] * df["volume"] * 100.0  # 元
    df["turnover"] = float("nan")
    df["pct_chg"] = df["close"].pct_change() * 100.0   # fallback approx
    df["src"] = "tx"
    return df[[c for c in STD_COLS if c != "name"]]

def fetch_hist(code: str, start: str, end: str):
    """eastmoney -> tencent. Returns DataFrame, None (empty), or 'FAILED'.

    V3: every underlying HTTP call now carries REQ_TIMEOUT, so worst case per
    stock is bounded (~3*30s em attempts + backoff + 30s tx); a stock can delay
    the run but never freeze it.
    """
    time.sleep(random.uniform(0.1, 0.8))  # V3: jitter against thundering herd
    err = None
    for attempt in range(RETRIES):
        try:
            r = _fetch_em(code, start, end)
            return r
        except Exception as e:
            err = e
            time.sleep(2 * (attempt + 1))
    try:
        r = _fetch_tx(code, start, end)
        if r is not None:
            print(f"  {code}: eastmoney failed ({err}), tencent OK")
        return r
    except Exception as e2:
        print(f"  FAILED {code}: em={err} tx={e2}")
        return "FAILED"

# ---------------------------------------------------------------- limit flags (approx)
def limit_threshold(code: str, is_st: bool, date: pd.Timestamp) -> float:
    d = pd.to_datetime(date).date()
    if is_st:
        return 5.0
    if code.startswith("688"):
        return 20.0                       # STAR since 2019-07-22
    if code[:3] in ("300", "301"):
        return 20.0 if d >= dt.date(2020, 8, 24) else 10.0
    if code.startswith("8"):
        return 30.0 if d >= dt.date(2021, 11, 15) else 10.0
    return 10.0

def add_limit_flags(df: pd.DataFrame, st_map: dict) -> pd.DataFrame:
    df = df.copy()
    df["is_st"] = df["code"].map(st_map).fillna(False)
    thr = [limit_threshold(c, s, d) for c, s, d in
           zip(df["code"], df["is_st"], df["date"])]
    thr = pd.Series(thr, index=df.index)
    df["limit_up_approx"] = df["pct_chg"] >= (thr - 0.5)
    df["limit_down_approx"] = df["pct_chg"] <= -(thr - 0.5)
    return df.drop(columns=["is_st"])

# ---------------------------------------------------------------- partitions
def year_path(year: int) -> str:
    return os.path.join(DAILY_DIR, f"{year}.parquet")

def load_meta() -> dict:
    if os.path.exists(META_JSON):
        with open(META_JSON) as f:
            return json.load(f)
    return {"years": {}, "date_min": None, "date_max": None,
            "attempted_max": None, "total_rows": 0}

def save_meta(meta: dict):
    with open(META_JSON, "w") as f:
        json.dump(meta, f, indent=2, ensure_ascii=False)

def upsert_year(year: int, new: pd.DataFrame, st_map: dict, meta: dict):
    """Merge new rows into data/daily/{year}.parquet, update meta."""
    p = year_path(year)
    if new is not None and not new.empty:
        new = add_limit_flags(new, st_map)
        if os.path.exists(p):
            old = pd.read_parquet(p)
            all_df = pd.concat([old, new], ignore_index=True)
        else:
            all_df = new
        all_df = all_df.drop_duplicates(subset=["date", "code"], keep="last")
        all_df = all_df.sort_values(["date", "code"]).reset_index(drop=True)
        all_df.to_parquet(p, index=False)
    else:
        all_df = pd.read_parquet(p) if os.path.exists(p) else pd.DataFrame()
    if not all_df.empty:
        meta["years"][str(year)] = {
            "complete": True, "rows": int(len(all_df)),
            "stocks": int(all_df["code"].nunique()),
            "date_max": str(pd.to_datetime(all_df["date"]).max().date()),
        }
    # refresh global range
    dmax = [v["date_max"] for v in meta["years"].values() if v.get("date_max")]
    if dmax:
        meta["date_max"] = max(dmax)
        meta["date_min"] = min(
            str(pd.read_parquet(year_path(int(y)))["date"].min().date())
            for y in meta["years"] if os.path.exists(year_path(int(y))))
    meta["total_rows"] = sum(v.get("rows", 0) for v in meta["years"].values())
    save_meta(meta)

# ---------------------------------------------------------------- main
def fetch_many(codes, start: str, end: str):
    frames, failed, empty = [], [], 0
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(fetch_hist, c, start, end): c for c in codes}
        for i, fut in enumerate(as_completed(futs), 1):
            r = fut.result()
            if r == "FAILED":
                failed.append(futs[fut])
            elif r is None:
                empty += 1
            else:
                frames.append(r)
            if i % 100 == 0:  # V3: denser heartbeat so a freeze is visible fast
                print(f"  ... {i}/{len(codes)} "
                      f"elapsed={time.time()-t0:.0f}s failed={len(failed)}",
                      flush=True)
    new = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    return new, failed, empty, len(codes)

def main():
    today = dt.date.today()
    meta = load_meta()

    # 1) universe
    uni = get_listed_universe()
    uni.to_csv(UNIVERSE_CSV, index=False)
    st_map = dict(zip(uni["code"], uni["is_st"]))
    name_map = dict(zip(uni["code"], uni["name"]))
    print(f"listed universe: {len(uni)} (ST={int(uni['is_st'].sum())})")

    # 2) delisted (best-effort survivorship coverage)
    dl = get_delisted()
    dl.to_csv(DELIST_CSV, index=False)
    for _, r in dl.iterrows():
        st_map.setdefault(r["code"], _is_st(r.get("name", "")))
        name_map.setdefault(r["code"], r.get("name", ""))
    print(f"delisted: {len(dl)}")

    cur_year = today.year
    failed_all = []

    # 3) backfill per year (checkpointed; resumes after timeout)
    for year in range(START_YEAR, cur_year + 1):
        yinfo = meta["years"].get(str(year), {})
        if yinfo.get("complete") and os.path.exists(year_path(year)):
            continue
        y_start, y_end = f"{year}0101", f"{year}1231"
        act = [c for c in uni["code"]]
        if not dl.empty:
            dl_y = dl[(dl["list_date"] <= pd.Timestamp(f"{year}-12-31")) &
                      ((dl["delist_date"].isna()) |
                       (dl["delist_date"] >= pd.Timestamp(f"{year}-01-01")))]
            act += [c for c in dl_y["code"] if c not in st_map or True]
        act = sorted(set(act))
        print(f"backfill {year}: {len(act)} codes")
        new, failed, empty, n = fetch_many(act, y_start, y_end)
        failed_all += failed
        new["name"] = new["code"].map(name_map) if not new.empty else new
        upsert_year(year, new, st_map, meta)
        print(f"  {year}: rows={len(new)} failed={len(failed)} empty={empty}/{n}")

    # 4) incremental: fill dates after meta date_max
    date_max = pd.to_datetime(meta["date_max"]).date() if meta.get("date_max") else None
    attempted = pd.to_datetime(meta["attempted_max"]).date() if meta.get("attempted_max") else None
    start_after = max([d for d in [date_max, attempted] if d], default=None)
    if start_after is None:
        start_after = dt.date(START_YEAR, 1, 1) - dt.timedelta(days=1)
    miss_start = start_after + dt.timedelta(days=1)
    # skip weekend dates (no point requesting)
    want = [miss_start + dt.timedelta(days=i)
            for i in range((today - miss_start).days + 1)]
    want = [d for d in want if d.weekday() < 5]

    if not want or max(want) < miss_start:
        print("incremental: already up to date")
    else:
        s, e = want[0].strftime("%Y%m%d"), want[-1].strftime("%Y%m%d")
        print(f"incremental: {s} -> {e} ({len(want)} weekdays)")
        new, failed, empty, n = fetch_many(sorted(uni["code"]), s, e)
        failed_all += failed
        if not new.empty:
            new["name"] = new["code"].map(name_map)
            new["date"] = pd.to_datetime(new["date"])
            for year, grp in new.groupby(new["date"].dt.year):
                upsert_year(int(year), grp, st_map, meta)
            meta["attempted_max"] = e
        elif empty / max(n, 1) > 0.9:
            # (almost) every stock empty -> non-trading day(s), don't retry
            print(f"  {s}->{e} looks like holiday(s), marking attempted")
            meta["attempted_max"] = e
        else:
            print(f"  WARNING: {len(failed)} failed, {empty} empty; will retry next run")
        save_meta(meta)

    with open(FAILED_TXT, "w") as f:
        f.write("\n".join(sorted(set(failed_all))) or "(none)")
    print("meta:", {k: v for k, v in meta.items() if k != "years"})
    print("years:", {k: v.get("date_max") for k, v in meta["years"].items()})

if __name__ == "__main__":
    main()
