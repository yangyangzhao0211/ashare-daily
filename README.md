# ashare-daily (V2)

全市场 A 股日线数据库：2019 年至今，按年分区永久保存，供交易系统 V1.0 每日任务与历史回测使用。

## 数据布局

```
data/
  daily/
    2019.parquet  ...  2026.parquet   # 每年一个分区，只追加不删除
  universe.csv     # 当前上市股票池：code / name / is_st
  delist.csv       # 已退市股票：code / name / list_date / delist_date
  meta.json        # 同步状态（每年是否 complete、数据起止日期）
```

## 字段（data/daily/YYYY.parquet）

| 字段 | 说明 |
|---|---|
| date/code/name | 日期 / 6位代码 / 名称 |
| open/high/low/close | 元，**未复权** |
| volume | 手；amount：元；turnover/pct_chg：% |
| pct_chg | 交易所官方涨跌幅（东财口径；腾讯备用源为收盘价推算，src 标记） |
| src | em=东方财富 / tx=腾讯 |
| limit_up_approx / limit_down_approx | 按涨停规则近似推导（主板10/ST5/创业板科创板20/北交所30，含2020-08-24、2021-11-15规则切换），研究便利字段，非官方标识 |

## 拉取地址（消费方）

```
https://raw.githubusercontent.com/<用户名>/<仓库名>/main/data/daily/2026.parquet
https://raw.githubusercontent.com/<用户名>/<仓库名>/main/data/meta.json
```

## 部署步骤

1. 新建仓库（建议公开：私有仓免费 Actions 时长可能不够；数据本身是公开行情）
2. 上传本目录 4 个文件，保持目录结构（`scripts/`、`.github/workflows/`）
3. **关键**：仓库 Settings → Actions → General → Workflow permissions → 选
   **Read and write permissions** → Save（否则 git push 会报权限错误）
4. Actions 页手动 Run workflow 一次做全量回填（2019 起，约 1-2 小时，断点续传）
5. 之后每天 16:30 北京时间自动增量

## 已知局限（诚实声明）

- 退市股历史为 best-effort：免费源无法保证 100% 覆盖，失败代码记在 `data/_failed.txt`
- V1 曾用 120 天滚动窗口，V2 改为按年永久保存，旧设计已废弃
