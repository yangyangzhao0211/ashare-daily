"""Offline fault/continuation tests. Synthetic fixtures are not source acceptance."""
import datetime as dt
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import ashare_core as core
import sync_daily as sync
from baostock_worker import collect


def fixtures(date="2025-01-03"):
    codes = [f"sh.{600000+i}" for i in range(110)] + ["sz.000001", "sz.300001", "sh.688001"]
    universe = pd.DataFrame({"code": codes, "tradeStatus": "1", "code_name": "fixture"})
    rows = pd.DataFrame({"date": date, "code": codes, "open": "10", "high": "11",
                         "low": "9", "close": "10.5", "preclose": "10", "volume": "10000",
                         "amount": "103000", "turn": "0.5", "pctChg": "5",
                         "tradestatus": "1", "isST": "0", "adjustflag": "3"})
    return rows, universe


class FakeClient:
    mismatch = False

    def __init__(self, **kwargs):
        pass

    def close(self):
        pass

    def call(self, op, **args):
        if op == "calendar":
            days = pd.date_range(args["start"], args["end"])
            return pd.DataFrame({"calendar_date": days.strftime("%Y-%m-%d"),
                                 "is_trading_day": [str(int(d.weekday() < 5)) for d in days]})
        rows, universe = fixtures(args["date"])
        if op == "universe":
            return universe
        if op == "sample":
            result = rows[rows.code == args["code"]].copy()
            if self.mismatch:
                result["amount"] = "9999999"
            return result
        return rows


class ValidationTests(unittest.TestCase):
    def setUp(self):
        self.raw, universe = fixtures()
        self.universe = core.universe_frame(universe)

    def validate(self, raw=None):
        return core.validate_day(self.raw if raw is None else raw, self.universe, "2025-01-03")

    def test_real_amount_and_units_preserved(self):
        frame, report = self.validate()
        self.assertEqual(frame.amount.iloc[0], 103000)
        self.assertNotEqual(frame.amount.iloc[0], frame.close.iloc[0]*frame.volume.iloc[0])
        self.assertTrue(report["source_universe_complete"])
        self.assertFalse(report["all_china_a_complete"])

    def test_missing_stock_is_partial(self):
        frame, report = self.validate(self.raw.iloc[1:])
        self.assertIsNone(frame)
        self.assertEqual(report["missing_active"], ["sh.600000"])

    def test_empty_wrong_date_duplicates_adjusted_fail(self):
        wrong_date = self.raw.assign(date="2025-01-02")
        duplicate = pd.concat([self.raw, self.raw.iloc[:1]], ignore_index=True)
        adjusted = self.raw.assign(adjustflag="2")
        for raw in (self.raw.iloc[:0], wrong_date, duplicate, adjusted):
            with self.subTest(size=len(raw)), self.assertRaises(ValueError):
                self.validate(raw)

    def test_invalid_prices_and_infinite_amount_fail(self):
        for column, value in (("high", "9"), ("amount", "inf"), ("close", ""), ("volume", "-1")):
            bad = self.raw.copy()
            bad.loc[0, column] = value
            with self.subTest(column=column), self.assertRaises(ValueError):
                self.validate(bad)

    def test_st_unknown_is_not_fabricated(self):
        frame, report = self.validate(self.raw.drop(columns="isST"))
        self.assertTrue(frame.is_st.isna().all())
        self.assertEqual(report["missing_st_flags"], len(frame))

    def test_suspended_stock_may_be_absent(self):
        self.universe.loc[0, "trade_status"] = 0
        frame, report = self.validate(self.raw.iloc[1:])
        self.assertIsNotNone(frame)
        self.assertTrue(report["source_universe_complete"])

    def test_beijing_and_indices_not_silently_counted(self):
        other = self.raw.iloc[:2].copy()
        other["code"] = ["bj.920001", "sh.000001"]
        frame, report = self.validate(pd.concat([self.raw, other]))
        self.assertEqual(len(frame), len(self.raw))
        self.assertFalse(report["all_china_a_complete"])

    def test_partial_refresh_does_not_replace_verified_shard(self):
        with tempfile.TemporaryDirectory() as folder:
            frame, quality = self.validate()
            core.save_day(folder, frame, quality)
            path = Path(folder)/"daily/2025/2025-01-03.parquet"
            before = path.read_bytes()
            result, report = self.validate(self.raw.iloc[1:])
            self.assertIsNone(result)
            self.assertEqual(before, path.read_bytes())
            self.assertTrue(core.valid_saved(folder, "2025-01-03"))
            path.write_bytes(b"corrupt")
            self.assertFalse(core.valid_saved(folder, "2025-01-03"))

    def test_state_corruption_does_not_reset(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder)/"state.json"
            path.write_text("invalid")
            with self.assertRaises(json.JSONDecodeError):
                core.read_json(path, {})

    def test_newest_first_and_cooldown_do_not_block_older_days(self):
        days = ["2025-01-01", "2025-01-02", "2025-01-03"]
        future = (dt.datetime.now(dt.timezone.utc)+dt.timedelta(days=1)).isoformat()
        state = {days[-1]: {"status": "partial", "next_retry_at": future}}
        self.assertEqual(core.task_order(days, state, "history"), [days[1], days[0]])

    def test_recent_refreshes_last_five_even_after_completion(self):
        days = ["2025-01-01", "2025-01-02", "2025-01-03"]
        state = {d: {"status": "complete"} for d in days}
        self.assertEqual(core.task_order(days, state, "recent"), list(reversed(days)))

    def test_truncated_calendar_rejected(self):
        raw = pd.DataFrame({"calendar_date": ["2025-01-01"], "is_trading_day": ["1"]})
        with self.assertRaises(ValueError):
            core.trading_days(raw, "2025-01-01", "2025-01-02")

    def test_exact_2000_daily_rows_do_not_paginate(self):
        class Result:
            error_code, fields, data = "0", ["code"], [[str(i)] for i in range(2000)]
            def next(self):
                raise AssertionError("Daily batch must not paginate")
        self.assertEqual(len(collect(Result(), single_page=True)["rows"]), 2000)

    def test_worker_hang_killed_by_parent_deadline(self):
        original = subprocess.Popen
        def hung_process(*args, **kwargs):
            return original([sys.executable, "-u", "-c", "import time; time.sleep(30)"], **kwargs)
        client = core.Client(timeout=0.1, retries=0)
        with patch("ashare_core.subprocess.Popen", side_effect=hung_process):
            with self.assertRaises(core.SourceError):
                client.call("calendar", start="2025-01-01", end="2025-01-02")
        self.assertIsNone(client.process)


class PipelineTests(unittest.TestCase):
    def run_pipeline(self, folder, mode, mismatch=False):
        folder = Path(folder)
        argv = ["sync_daily.py", "--mode", mode, "--end-date", "2025-01-10", "--max-days", "2"]
        with patch.multiple(sync, ROOT=folder, DATA=folder/"data/v5", REPORTS=folder/"reports/v5",
                            STATE_PATH=folder/"data/v5/state.json", STOP=False), \
             patch("sync_daily.Client", FakeClient), patch.object(FakeClient, "mismatch", mismatch), \
             patch("sys.argv", argv), patch("sync_daily.time.sleep"), patch("sync_daily.log"):
            return sync.main()

    def test_gate_and_resume_across_runs(self):
        with tempfile.TemporaryDirectory() as folder:
            self.assertEqual(self.run_pipeline(folder, "probe"), 0)
            self.assertEqual(self.run_pipeline(folder, "history"), 0)
            state = core.read_json(Path(folder)/"data/v5/state.json")
            first = {d for d, task in state["tasks"].items() if task["status"] == "complete"}
            self.assertEqual(len(first), 2)
            self.assertEqual(self.run_pipeline(folder, "history"), 0)
            state = core.read_json(Path(folder)/"data/v5/state.json")
            second = {d for d, task in state["tasks"].items() if task["status"] == "complete"}
            self.assertEqual(len(second), 4)
            self.assertTrue(first.issubset(second))
            self.assertTrue((Path(folder)/".recovery_v5/last-run.zip").exists())

    def test_probe_mismatch_blocks_collection(self):
        with tempfile.TemporaryDirectory() as folder:
            self.assertEqual(self.run_pipeline(folder, "history", mismatch=True), 2)
            state = core.read_json(Path(folder)/"data/v5/state.json")
            self.assertFalse(state["gate"]["passed"])
            self.assertFalse(state["tasks"])
            self.assertFalse(list((Path(folder)/"data/v5/daily").glob("**/*.parquet")))

    def test_push_failure_is_nonzero_for_probe(self):
        with tempfile.TemporaryDirectory() as folder, patch("sync_daily.git_checkpoint", side_effect=RuntimeError("push denied")):
            self.assertEqual(self.run_pipeline(folder, "probe"), 2)


if __name__ == "__main__":
    unittest.main(verbosity=2)
