#!/usr/bin/env python3
"""V5 Path A: probe -> serial full-market-by-date requests -> verified shards."""
import argparse
import datetime as dt
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import zipfile
from zoneinfo import ZoneInfo

import pandas as pd

from ashare_core import (Client, GATE_ID, NUMERIC, SCOPE, VERSION, SourceError,
                        atomic_json, log, read_json, save_day, task_order,
                        trading_days, universe_frame, utc_now, valid_saved, validate_day)

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data" / "v5"
REPORTS = ROOT / "reports" / "v5"
STATE_PATH = DATA / "state.json"
STOP = False


def stop_signal(signum, frame):
    global STOP
    STOP = True
    log(f"Signal {signum} received: finish current request and persist state")


def git_checkpoint(enabled):
    if not enabled:
        return
    # Only new verified partitions and metadata belong to this pipeline.
    commands = [["git", "add", "--", "data/v5", "reports/v5"]]
    for command in commands:
        subprocess.run(command, cwd=ROOT, check=True, timeout=30)
    changed = subprocess.run(["git", "diff", "--cached", "--quiet"], cwd=ROOT).returncode
    if changed == 0:
        return
    if changed != 1:
        raise RuntimeError("Cannot inspect staged checkpoint")
    subprocess.run(["git", "commit", "-m", f"A-share V5 checkpoint {utc_now()}"],
                   cwd=ROOT, check=True, timeout=60)
    subprocess.run(["git", "push"], cwd=ROOT, check=True, timeout=90)
    log("Checkpoint pushed")


def choose_probe_days(days):
    choices = []
    for year in (2019, 2022, 2025):
        year_days = [d for d in days if d.startswith(str(year))]
        if year_days:
            choices.append(year_days[0])
    choices.extend(days[-2:])
    return list(dict.fromkeys(choices))


def sample_codes(frame):
    active = frame[frame.trade_status == 1]
    choices = []
    for prefix in ("sh.60", "sz.00", "sz.30", "sh.688"):
        codes = active.loc[active.code.str.startswith(prefix), "code"]
        if len(codes):
            choices.append(codes.iloc[0])
    st = active.loc[active.is_st.eq(1).fillna(False), "code"]
    if len(st):
        choices.append(st.iloc[0])
    return list(dict.fromkeys(choices))


def compare_sample(client, daily, universe, date, code):
    sample_raw = client.call("sample", date=date, code=code)
    one_universe = universe[universe.code == code]
    # Reuse all numeric/basis checks without the universe's minimum-size rule.
    sample, report = validate_day(sample_raw, one_universe, date)
    if sample is None or len(sample) != 1:
        raise ValueError(f"Single-stock sample missing: {date} {code}")
    row = daily.set_index("code").loc[code]
    other = sample.iloc[0]
    for field in NUMERIC:
        left, right = float(row[field]), float(other[field])
        tolerance = max(1e-6, abs(right) * 1e-6)
        if abs(left - right) > tolerance:
            raise ValueError(f"Batch/single-stock mismatch {date} {code} {field}: {left} vs {right}")
    if pd.notna(row.is_st) and (pd.isna(other.is_st) or row.is_st != other.is_st):
        raise ValueError(f"ST status mismatch: {date} {code}")
    return {"code": code, "numeric_fields_match": True,
            "unadjusted_price_basis_checked": True}


def run_probe(client, days, state, deadline):
    # Invalidate previous approval before attempting a new probe.
    state["gate"] = {"id": GATE_ID, "passed": False, "started_at": utc_now()}
    atomic_json(STATE_PATH, state)
    report = {"version": VERSION, "scope": SCOPE, "passed": False,
              "samples": [], "all_china_a_complete": False, "started_at": utc_now()}
    try:
        for date in choose_probe_days(days):
            if STOP or time.monotonic() >= deadline:
                raise RuntimeError("Probe time budget reached; gate stays closed")
            log(f"PROBE {date}: historical universe and full-market daily")
            universe = universe_frame(client.call("universe", date=date))
            frame, quality = validate_day(client.call("daily", date=date), universe, date)
            if frame is None:
                raise ValueError(f"Probe coverage failed: {json.dumps(quality, ensure_ascii=False)}")
            checks = []
            for code in sample_codes(frame):
                if STOP or time.monotonic() >= deadline:
                    raise RuntimeError("Probe deadline reached during cross-check")
                checks.append(compare_sample(client, frame, universe, date, code))
            if not checks:
                raise ValueError("Probe has no active cross-check samples")
            report["samples"].append({"date": date, "quality": quality, "cross_checks": checks})
        report.update(passed=True, completed_at=utc_now(),
                      note="Verified against BaoStock historical universe and single-stock endpoint; not independent vendor validation")
        state["gate"] = {"id": GATE_ID, "passed": True, "checked_at": utc_now(),
                         "sample_dates": [s["date"] for s in report["samples"]], "scope": SCOPE}
        log("PROBE PASSED: Path A enabled for the historical Shanghai/Shenzhen source universe")
    except Exception as exc:
        report.update(error=str(exc), completed_at=utc_now())
        state["gate"].update(error=str(exc), checked_at=utc_now())
        log(f"PROBE FAILED: {exc}. Historical downloading remains disabled.")
        raise
    finally:
        atomic_json(REPORTS / "probe.json", report)
        atomic_json(STATE_PATH, state)


def gate_valid(state):
    gate = state.get("gate", {})
    if gate.get("id") != GATE_ID or not gate.get("passed"):
        return False
    checked = dt.datetime.fromisoformat(gate["checked_at"])
    return dt.datetime.now(dt.timezone.utc) - checked < dt.timedelta(days=7)


def rebuild_summary():
    """Read only quality-approved V5 partitions; never merge legacy V4 data."""
    rows = []
    for path in sorted((DATA / "quality").glob("*.json")):
        quality = read_json(path)
        date = quality["date"]
        if not valid_saved(DATA, date):
            continue
        rows.append({"date": date, "scope": SCOPE, **quality["breadth"],
                     "source_universe_complete": True, "all_china_a_complete": False})
    if rows:
        temp = DATA / "market_daily.parquet.tmp"
        pd.DataFrame(rows).to_parquet(temp, index=False, compression="zstd")
        os.replace(temp, DATA / "market_daily.parquet")
    return len(rows)


def make_recovery(paths):
    directory = ROOT / ".recovery_v5"
    directory.mkdir(exist_ok=True)
    with zipfile.ZipFile(directory / "last-run.zip", "w", zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(set(paths) | {STATE_PATH} | set(REPORTS.rglob("*.json"))):
            if path.exists():
                archive.write(path, path.relative_to(ROOT))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["probe", "recent", "history", "repair"],
                        default=os.getenv("MODE", "recent"))
    parser.add_argument("--start-date", default=os.getenv("START_DATE", "2019-01-01"))
    parser.add_argument("--end-date", default=None, help="Optional historical end date, YYYY-MM-DD")
    parser.add_argument("--budget-minutes", type=float, default=float(os.getenv("RUN_MINUTES", "30")))
    parser.add_argument("--max-days", type=int, default=int(os.getenv("MAX_DAYS", "100")))
    parser.add_argument("--git-checkpoint", action="store_true",
                        default=os.getenv("GIT_CHECKPOINT", "0") == "1")
    args = parser.parse_args()
    if args.budget_minutes <= 0 or args.max_days <= 0:
        parser.error("Budget and max-days must be positive")
    dt.date.fromisoformat(args.start_date)
    for directory in (DATA, REPORTS):
        directory.mkdir(parents=True, exist_ok=True)
    state = read_json(STATE_PATH, {"version": VERSION, "tasks": {}, "gate": {}})
    if state.get("version") != VERSION:
        raise RuntimeError("Unsupported state version; keep old states separate")
    state.setdefault("tasks", {})
    started = time.monotonic()
    deadline = started + args.budget_minutes * 60
    client = Client(timeout=float(os.getenv("REQUEST_TIMEOUT", "60")), retries=2)
    recovered_paths = []
    summary = {"version": VERSION, "mode": args.mode, "scope": SCOPE,
               "started_at": utc_now(), "completed_this_run": [], "issues_this_run": [],
               "all_china_a_complete": False, "outcome": "running"}
    code = 0
    last_checkpoint, since_checkpoint = started, 0
    try:
        now = dt.datetime.now(ZoneInfo("Asia/Shanghai"))
        # Before 21:00 only use yesterday or earlier; do not mark intraday data as final.
        safe_end = now.date() if now.hour >= 21 else now.date() - dt.timedelta(days=1)
        end = min(dt.date.fromisoformat(args.end_date), safe_end) if args.end_date else safe_end
        if args.start_date > str(end):
            raise ValueError("Start date is after the latest safe end date")
        log(f"V5 Path A mode={args.mode}; calendar {args.start_date}..{end}; scope={SCOPE}")
        calendar = client.call("calendar", start=args.start_date, end=str(end))
        days = trading_days(calendar, args.start_date, str(end))
        if not days:
            raise ValueError("No trading dates in requested interval")
        atomic_json(DATA / "calendar.json", {"start": args.start_date, "end": str(end),
                                               "trading_days": days, "fetched_at": utc_now()})
        if args.mode == "probe" or not gate_valid(state):
            run_probe(client, days, state, min(deadline, started + 10 * 60))
            git_checkpoint(args.git_checkpoint)
        if args.mode == "probe":
            summary["outcome"] = "probe_passed"
        else:
            # Recover file-before-state interruptions, and detect missing/corrupt shards.
            for date in days:
                if valid_saved(DATA, date):
                    state["tasks"].setdefault(date, {})["status"] = "complete"
                elif state["tasks"].get(date, {}).get("status") == "complete":
                    state["tasks"][date].update(status="partial", reason="Saved shard/checksum missing")
            pending = task_order(days, state["tasks"], args.mode,
                                 int(os.getenv("RECENT_TRADING_DAYS", "120")), 5)
            log(f"Queued {len(pending)} trading days; newest first; max {args.max_days} this run")
            consecutive_errors = 0
            for date in pending[:args.max_days]:
                # Reserve 3 minutes for report/recovery/checkpoint work.
                if STOP or time.monotonic() >= deadline - 180:
                    log("Soft deadline reached; remaining dates will continue next run")
                    break
                task = state["tasks"].setdefault(date, {})
                old_complete = task.get("status") == "complete" and valid_saved(DATA, date)
                task["attempts"] = int(task.get("attempts", 0)) + 1
                log(f"DAY {date}, attempt={task['attempts']}")
                try:
                    universe = universe_frame(client.call("universe", date=date))
                    frame, quality = validate_day(client.call("daily", date=date), universe, date)
                    if frame is None:
                        status, reason = "partial", quality["reason"]
                        atomic_json(REPORTS / "dates" / f"{date}.json", quality)
                        summary["issues_this_run"].append({"date": date, "status": status,
                                                         "reason": reason, "missing": quality["missing_active"]})
                        if not old_complete:
                            task.update(status=status, reason=reason)
                        else:
                            task.update(refresh_issue=reason)
                        task["next_retry_at"] = (dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=6)).isoformat()
                        consecutive_errors += 1
                    else:
                        quality = save_day(DATA, frame, quality)
                        task.update(status="complete", checked_at=utc_now(), rows=len(frame))
                        for key in ("next_retry_at", "reason", "refresh_issue"):
                            task.pop(key, None)
                        summary["completed_this_run"].append(date)
                        recovered_paths.extend([DATA / "daily" / date[:4] / f"{date}.parquet",
                                                DATA / "quality" / f"{date}.json"])
                        consecutive_errors = 0
                        log(f"SAVED {date}: {len(frame)} rows, expected active={quality['expected_active']}")
                except (SourceError, ValueError) as exc:
                    # Keep a previously verified shard during an unsuccessful refresh.
                    if not old_complete:
                        task.update(status="retryable_error" if isinstance(exc, SourceError) and not exc.permanent else "partial",
                                    reason=str(exc))
                    else:
                        task.update(refresh_issue=str(exc))
                    delay_hours = min(24, 2 ** min(task["attempts"], 4))
                    task["next_retry_at"] = (dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=delay_hours)).isoformat()
                    summary["issues_this_run"].append({"date": date, "reason": str(exc)})
                    log(f"UNRESOLVED {date}: {exc}")
                    consecutive_errors += 1
                    if isinstance(exc, SourceError) and exc.permanent:
                        state["gate"]["passed"] = False
                        raise
                state["updated_at"] = utc_now()
                atomic_json(STATE_PATH, state)
                since_checkpoint += 1
                if since_checkpoint >= 5 or time.monotonic() - last_checkpoint >= 240:
                    atomic_json(REPORTS / "last_run.json", summary)
                    git_checkpoint(args.git_checkpoint)
                    last_checkpoint, since_checkpoint = time.monotonic(), 0
                if consecutive_errors >= 5:
                    raise RuntimeError("Five consecutive unresolved days; circuit breaker stopped this run")
                time.sleep(float(os.getenv("REQUEST_PAUSE", "1")))
            complete = sum(state["tasks"].get(d, {}).get("status") == "complete" for d in days)
            summary.update(verified_days=complete, target_days=len(days), remaining_days=len(days) - complete,
                           latest_target_trade_date=days[-1],
                           outcome="completed_with_gaps" if summary["issues_this_run"] else "batch_completed")
            if summary["issues_this_run"]:
                code = 3
            rebuild_summary()
            log(f"PROGRESS: verified={complete}/{len(days)}, new={len(summary['completed_this_run'])}, issues={len(summary['issues_this_run'])}")
    except Exception as exc:
        summary.update(outcome="failed", error=str(exc))
        log(f"STOPPED: {exc}")
        code = 2
    finally:
        client.close()
        summary["finished_at"] = utc_now()
        summary["elapsed_minutes"] = round((time.monotonic() - started) / 60, 2)
        atomic_json(STATE_PATH, state)
        atomic_json(REPORTS / "last_run.json", summary)
        make_recovery(recovered_paths)
        step_summary = os.getenv("GITHUB_STEP_SUMMARY")
        if step_summary:
            with open(step_summary, "a", encoding="utf-8") as output:
                output.write("\n### A-share V5 Path A\n\n```json\n" + json.dumps(summary, ensure_ascii=False, indent=2) + "\n```\n")
        try:
            git_checkpoint(args.git_checkpoint)
        except Exception as exc:
            log(f"Final push failed: {exc}; download v5-recovery artifact before retrying")
            code = 2
    return code


if __name__ == "__main__":
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, stop_signal)
    sys.exit(main())
