# ashare-daily

全市场 A 股日线自动同步（供交易系统 V1.0 每日盘前/盘后任务使用）。

## 数据

- `data/daily_latest.parquet`：全市场滚动 120 天日线（date/code/open/high/low/close/volume/amount/turnover/pct_chg）
  - 价格为**未复权**；`pct_chg` 为交易所官方涨跌幅（做涨停/连板判断直接用它，不用价格算）
  - `volume` 单位：手；`amount` 单位：元；`turnover`/`pct_chg` 单位：%
- `data/universe.csv`：股票清单（code/name/is_st）
- `data/meta.json`：同步时间戳与数据起止日期

## 拉取地址（给消费方）

```
https://raw.githubusercontent.com/<你的用户名>/<仓库名>/main/data/daily_latest.parquet
https://raw.githubusercontent.com/<你的用户名>/<仓库名>/main/data/meta.json
```

## 说明

- 每天 16:30 北京时间自动增量同步（GitHub Actions 定时任务）
- 首次运行从 2019-01-01 全量回填（含前量化时代对照），之后每天只拉新增日期
- 涨停阈值（用 `pct_chg` 判断）：主板 9.5 / ST 4.5 / 创业板·科创板 19.0 / 北交所 29.0
