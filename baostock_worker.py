"""Isolated, serial BaoStock session. JSON lines are the only stdout output."""
import contextlib
import json
import os
import socket
import sys

socket.setdefaulttimeout(float(os.getenv("SOCKET_TIMEOUT", "20")))


class SourceError(Exception):
    def __init__(self, code, message):
        super().__init__(message)
        self.code = str(code)


def check(result):
    if str(result.error_code) != "0":
        raise SourceError(result.error_code, result.error_msg)


def collect(result, single_page=False):
    check(result)
    if single_page:
        # This endpoint returns all rows in one message. Avoid the SDK's
        # next() accidentally paginating a response with exactly 2,000 rows.
        return {"fields": result.fields, "rows": result.data}
    rows = []
    while True:
        rows.extend(result.data)
        result.cur_row_num = len(result.data)
        old_page = str(result.cur_page_num)
        full_page = len(result.data) == 2000
        has_next = result.next()
        check(result)
        if not has_next:
            if full_page and str(result.cur_page_num) == old_page:
                raise SourceError("pagination", "Full page ended without a confirmed next-page response")
            break
        if str(result.cur_page_num) == old_page:
            raise SourceError("pagination", "Pagination made no progress")
    return {"fields": result.fields, "rows": rows}


def serve():
    with contextlib.redirect_stdout(sys.stderr):
        import baostock as bs
    connected = False
    for line in sys.stdin:
        try:
            args = json.loads(line)
            with contextlib.redirect_stdout(sys.stderr):
                if not connected:
                    key = os.getenv("BAOSTOCK_API_KEY", "").strip()
                    if key:
                        bs.set_API_key(key)
                    check(bs.login())
                    connected = True
                op = args["op"]
                if op == "calendar":
                    result = bs.query_trade_dates(args["start"], args["end"])
                elif op == "universe":
                    result = bs.query_all_stock(args["date"])
                elif op == "daily":
                    result = bs.query_daily_history_k_AStock(args["date"])
                elif op == "sample":
                    result = bs.query_history_k_data_plus(
                        args["code"],
                        "date,code,open,high,low,close,preclose,volume,amount,turn,tradestatus,pctChg,isST,adjustflag",
                        start_date=args["date"], end_date=args["date"], frequency="d", adjustflag="3")
                else:
                    raise ValueError(f"Unknown operation: {op}")
                payload = collect(result, single_page=(op == "daily"))
            reply = {"ok": True, **payload}
        except Exception as exc:
            connected = False
            reply = {"ok": False, "code": getattr(exc, "code", "client_error"), "error": str(exc)}
        print(json.dumps(reply, ensure_ascii=False), flush=True)
    if connected:
        with contextlib.redirect_stdout(sys.stderr):
            bs.logout()


if __name__ == "__main__":
    serve()
