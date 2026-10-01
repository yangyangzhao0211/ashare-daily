"""Daily-path validation, storage, and a killable BaoStock client."""
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import queue
import re
import subprocess
import sys
import threading
import time

import pandas as pd

VERSION = "5.0"
SCOPE = "BaoStock historical Shanghai/Shenzhen A-share universe; Beijing excluded"
GATE_ID = "v5.0-shsz-unadjusted-strict-active-coverage"
PERMANENT_CODES = {"10001004", "10001006", "10001007", "10001011", "10001012",
                   "10004013", "10004015", "10004020"}
A_SHARE = re.compile(r"^(sh\.(?:60\d{4}|688\d{3})|sz\.(?:00\d{4}|30\d{4}))$")
NUMERIC = ["open", "high", "low", "close", "preclose", "volume", "amount", "turnover", "pct_chg"]
ALIASES = {"pre_close": "preclose", "turn": "turnover", "pctChg": "pct_chg",
           "tradeStatus": "trade_status", "tradestatus": "trade_status", "isST": "is_st"}


def utc_now():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def log(message):
    print(f"[{utc_now()}] {message}", flush=True)


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
    os.replace(temp, path)


def read_json(path, default=None):
    path = Path(path)
    if not path.exists():
        return default
    # Corrupt state is a hard error; never silently start from scratch.
    return json.loads(path.read_text(encoding="utf-8"))


class SourceError(RuntimeError):
    def __init__(self, code, message):
        super().__init__(f"{code}: {message}")
        self.code = str(code)

    @property
    def permanent(self):
        return self.code in PERMANENT_CODES


class Client:
    def __init__(self, timeout=60, retries=2):
        self.timeout, self.retries = timeout, retries
        self.process = None
        self.messages = None

    def start(self):
        self.messages = queue.Queue()
        self.process = subprocess.Popen(
            [sys.executable, "-u", str(Path(__file__).with_name("baostock_worker.py"))],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=None,
            text=True, encoding="utf-8", bufsize=1)
        process, messages = self.process, self.messages

        def reader():
            for line in process.stdout:
                messages.put(line)
            messages.put(None)

        threading.Thread(target=reader, daemon=True).start()

    def close(self):
        if self.process is not None:
            if self.process.poll() is None:
                self.process.terminate()
                try:
                    self.process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    self.process.kill()
                    self.process.wait(timeout=3)
            for stream in (self.process.stdin, self.process.stdout):
                stream.close()
            self.process = None

    def call(self, op, **args):
        for attempt in range(self.retries + 1):
            try:
                if self.process is None or self.process.poll() is not None:
                    self.close()
                    self.start()
                self.process.stdin.write(json.dumps({"op": op, **args}) + "\n")
                self.process.stdin.flush()
                line = self.messages.get(timeout=self.timeout)
                if line is None:
                    raise SourceError("worker_exit", "BaoStock worker exited")
                result = json.loads(line)
                if not result["ok"]:
                    raise SourceError(result["code"], result["error"])
                return pd.DataFrame(result["rows"], columns=result["fields"])
            except (queue.Empty, BrokenPipeError, OSError, ValueError, SourceError) as exc:
                self.close()
                if isinstance(exc, SourceError) and exc.permanent:
                    raise
                if attempt >= self.retries:
                    if isinstance(exc, SourceError):
                        raise
                    raise SourceError("timeout_or_transport", str(exc) or "request deadline exceeded") from exc
                log(f"Request {op} failed; reconnecting ({attempt + 1}/{self.retries}): {exc}")
                time.sleep(2 * (attempt + 1))


def trading_days(raw, start, end):
    if not {"calendar_date", "is_trading_day"}.issubset(raw.columns):
        raise ValueError("Calendar is empty or has unknown fields")
    dates = pd.to_datetime(raw["calendar_date"], errors="coerce")
    if dates.isna().any() or raw.calendar_date.duplicated().any():
        raise ValueError("Invalid/duplicate calendar dates")
    expected = set(pd.date_range(start, end).strftime("%Y-%m-%d"))
    if set(dates.dt.strftime("%Y-%m-%d")) != expected:
        raise ValueError("Calendar is truncated or does not cover the requested interval")
    if not raw.is_trading_day.astype(str).isin(["0", "1"]).all():
        raise ValueError("Unknown calendar flags")
    return sorted(dates[raw.is_trading_day.astype(str) == "1"].dt.strftime("%Y-%m-%d"))


def universe_frame(raw):
    raw = raw.rename(columns=ALIASES).copy()
    if not {"code", "trade_status"}.issubset(raw.columns):
        raise ValueError("Historical universe has no code/tradeStatus")
    raw["code"] = raw.code.astype(str).str.lower()
    raw = raw[raw.code.str.match(A_SHARE)].copy()
    if len(raw) < 100 or raw.code.duplicated().any():
        raise ValueError("Historical universe empty, too small, or duplicated")
    if not raw.trade_status.astype(str).isin(["0", "1"]).all():
        raise ValueError("Universe trade status is unknown")
    raw["trade_status"] = raw.trade_status.astype(int)
    if "code_name" not in raw:
        raw["code_name"] = None
    return raw[["code", "trade_status", "code_name"]].reset_index(drop=True)


def validate_day(raw, universe, date):
    """Require every expected active source-universe stock; never invent fields."""
    x = raw.rename(columns=ALIASES).copy()
    if not {"date", "code", *NUMERIC}.issubset(x.columns):
        raise ValueError(f"Daily fields missing: {sorted({'date', 'code', *NUMERIC} - set(x.columns))}")
    x["code"] = x.code.astype(str).str.lower()
    x = x[x.code.str.match(A_SHARE)].copy()
    if x.empty:
        raise ValueError("empty_unverified: no Shanghai/Shenzhen A-share rows")
    parsed = pd.to_datetime(x.date, errors="coerce")
    if parsed.isna().any() or not parsed.dt.strftime("%Y-%m-%d").eq(date).all():
        raise ValueError("Response date differs from requested trading day")
    if x.code.duplicated().any():
        raise ValueError("Duplicate stock rows in response")
    x["date"] = parsed
    known = set(universe.code)
    expected = set(universe.loc[universe.trade_status == 1, "code"])
    received = set(x.code)
    missing, unexpected = sorted(expected - received), sorted(received - known)
    report = {"date": date, "scope": SCOPE, "expected_active": len(expected),
              "universe_stocks": len(known), "received_stocks": len(received),
              "missing_active": missing, "unexpected_codes": unexpected,
              "source_universe_complete": False, "all_china_a_complete": False}
    if not expected:
        raise ValueError("No active stocks in historical universe")
    if missing or unexpected:
        return None, {**report, "status": "partial", "reason": "Historical active-universe mismatch"}
    status_map = universe.set_index("code").trade_status
    if "trade_status" in x:
        supplied = pd.to_numeric(x.trade_status, errors="coerce")
        if supplied.isna().any() or not supplied.eq(x.code.map(status_map)).all():
            raise ValueError("Daily trade status disagrees with historical universe")
    x["trade_status"] = x.code.map(status_map).astype(int)
    if "adjustflag" in x and not pd.to_numeric(x.adjustflag, errors="coerce").eq(3).all():
        raise ValueError("Daily endpoint returned adjusted/unknown price basis")
    for col in NUMERIC:
        original = x[col].replace("", None)
        x[col] = pd.to_numeric(original, errors="coerce")
        invalid = original.notna() & (x[col].isna() | x[col].isin([float("inf"), -float("inf")]))
        if invalid.any():
            raise ValueError(f"Non-numeric/non-finite {col}")
    active = x[x.trade_status == 1]
    if active[NUMERIC].isna().any().any():
        raise ValueError("Active rows have missing required numeric fields")
    if (active[["open", "high", "low", "close", "preclose"]] <= 0).any().any():
        raise ValueError("Active rows have non-positive prices")
    if (active[["volume", "amount", "turnover"]] < 0).any().any():
        raise ValueError("Negative volume/amount/turnover")
    if ((active.high < active[["open", "close", "low"]].max(axis=1)) |
        (active.low > active[["open", "close", "high"]].min(axis=1))).any():
        raise ValueError("OHLC ordering violated")
    if "is_st" not in x:
        x["is_st"] = pd.NA
    st_original = x.is_st.replace("", None)
    st_numeric = pd.to_numeric(st_original, errors="coerce")
    if (st_original.notna() & st_numeric.isna()).any():
        raise ValueError("Invalid historical ST flag")
    x["is_st"] = st_numeric.astype("Int64")
    if not x.is_st.dropna().isin([0, 1]).all():
        raise ValueError("Unknown historical ST values")
    x["name"] = x.code.map(universe.set_index("code").code_name)
    x["exchange"] = x.code.str[:2]
    x["symbol"] = x.code.str[3:]
    x["adjustflag"] = 3
    x["src"] = "baostock_daily_AStock"
    x["fetched_at"] = utc_now()
    columns = ["date", "code", "symbol", "exchange", "name", *NUMERIC,
               "trade_status", "is_st", "adjustflag", "src", "fetched_at"]
    report.update(status="complete", source_universe_complete=True,
                  missing_st_flags=int(x.is_st.isna().sum()), active_rows=len(active),
                  reason="All expected active stocks of the historical source universe received")
    return x[columns].sort_values("code").reset_index(drop=True), report


def save_day(root, frame, report):
    root = Path(root)
    date = report["date"]
    path = root / "daily" / date[:4] / f"{date}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(".parquet.tmp")
    frame.to_parquet(temp, index=False, compression="zstd")
    os.replace(temp, path)
    active = frame[frame.trade_status == 1]
    report = {**report, "rows": len(frame), "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
              "breadth": {"stocks": len(active), "up_count": int((active.pct_chg > 0).sum()),
                          "down_count": int((active.pct_chg < 0).sum()),
                          "flat_count": int((active.pct_chg == 0).sum()),
                          "total_amount_yuan": float(active.amount.sum()),
                          "median_pct_chg": float(active.pct_chg.median())},
              "validated_at": utc_now()}
    atomic_json(root / "quality" / f"{date}.json", report)
    return report


def valid_saved(root, date):
    root = Path(root)
    report = read_json(root / "quality" / f"{date}.json")
    path = root / "daily" / date[:4] / f"{date}.parquet"
    return bool(report and report.get("source_universe_complete") and path.exists()
                and hashlib.sha256(path.read_bytes()).hexdigest() == report.get("sha256"))


def task_order(days, state, mode, recent_days=120, repair_days=5):
    if mode == "recent":
        priority = list(reversed(days[-repair_days:]))
        backlog = [d for d in reversed(days[-recent_days:]) if state.get(d, {}).get("status") != "complete"]
    elif mode == "repair":
        priority = []
        backlog = [d for d in reversed(days) if state.get(d, {}).get("status") not in (None, "complete")]
    else:
        priority = []
        backlog = [d for d in reversed(days) if state.get(d, {}).get("status") != "complete"]
    now = dt.datetime.now(dt.timezone.utc)
    result = []
    for d in dict.fromkeys(priority + backlog):
        retry = state.get(d, {}).get("next_retry_at")
        if retry and dt.datetime.fromisoformat(retry) > now:
            continue
        result.append(d)
    return result
