# A股历史日线 V5.0：路径 A（按交易日下载）

此版本用于已有的 `ashare-daily` 仓库。默认从 2019-01-01 开始，使用 BaoStock 0.9.4 的 `query_daily_history_k_AStock(date)`，每次读取一个历史交易日的股票日线。先通过验收，再下载。只采用路径 A；接口无权限或历史不支持时明确停止，不自动转成逐股回补。

## 1. 先替换/添加哪些文件

先取消正在运行的旧采集任务，暂停旧工作流的定时运行。保留旧代码备份和整个原有 `data/` 目录。

| 文件 | 操作 | 放置位置 |
|---|---|---|
| `sync_daily.py` | 替换旧文件 | 仓库根目录 |
| `requirements.txt` | 替换旧文件 | 仓库根目录 |
| `market_daily.py` | 替换或新增 | 仓库根目录 |
| `ashare_core.py` | 新增 | 仓库根目录 |
| `baostock_worker.py` | 新增 | 仓库根目录 |
| `.github/workflows/ashare-path-a.yml` | 新增 | 必须是仓库根目录下的这个完整路径 |
| `tests/test_path_a.py` | 新增，可保留供验证 | `tests/` 目录 |
| `README_V5_使用说明.md` | 新增 | 仓库根目录 |
| `.gitignore-v5-snippet.txt` | 内容追加到原 `.gitignore` | 不要替换原 `.gitignore` |

压缩包里的 `ashare-daily-V5.0` 是外层目录。**把它里面的文件放到仓库根目录，不要把整个外层目录上传到仓库里。** 启用显示隐藏文件，确保 `.github` 文件夹一起复制。

旧工作流包括 `.github/workflows/daily-sync.yml`、`history-backfill.yml`、`daily-sync-v4.yml`、`history-backfill-v4.yml` 等。可以在 GitHub Actions 列表中逐个点击工作流 → 右侧菜单 → Disable workflow；更稳妥的方式是备份后将旧 yml 移出 `.github/workflows/`。V5 只保留一个定时采集工作流，避免旧任务继续写入。

如果 GitHub 提示 `git push` 无权限，进入 Settings → Actions → General → Workflow permissions，确认仓库允许工作流读写；组织策略或受保护分支仍可能阻止机器人提交。不要开启强制推送，修正正常写入权限后再运行。

## 2. 第一次只运行 probe

在 GitHub Actions 中打开 **A-share V5 Path A** → Run workflow：

1. 分支选择仓库默认分支。
2. `mode` 选择 `probe`。
3. 点击 Run workflow。
4. 查看运行日志、Summary 和 `v5-reports-...` artifact。

probe 对 2019、2022、2025 年的首个交易日及最近两个已完成交易日做检查；若自定义区间不包含这些年份，只检查区间内可用日期。每个日期核对历史名单，并抽取主板、创业板、科创板及可用的 ST 样本，与个股**不复权**日线比对开高低收、前收、真实成交量/成交额、换手率和涨跌幅。

验收通过必须看到 `PROBE PASSED`，且 `reports/v5/probe.json` 的 `passed` 是 `true`。验收门禁有效期为 7 天；过期后，下次采集先重新验收。验证码式价格抽样是同一数据商两个接口的一致性检查，并非独立数据商核验，也不保证所有历史字段绝对准确。

默认使用匿名公共访问，不需要填写密钥。若返回 `10001006`（权限不足）、`10004013`（超出日期支持范围）等，**本方案验收失败，不能声称已经免费取得全历史权限**。可选 `BAOSTOCK_API_KEY` secret 仅供你已有且授权使用的凭据；代码不会替你购买权限或绕过限制。

## 3. 通过后如何运行

先手动运行 `recent`，再运行 `history`。本轮最多尝试 100 个交易日，且采集预算约 30 分钟；达到任一限制就保存进度。后续手动或定时执行继续补齐，不需要等待所有历史在一轮结束。

| mode | 作用 |
|---|---|
| `probe` | 只验收接口、覆盖和基础字段 |
| `recent` | 最近 120 个交易日优先；每轮重新检查最近 5 个交易日 |
| `history` | 从最新日期倒序补齐，直至 2019；跳过已校验保存的数据 |
| `repair` | 重试已有的失败/不完整记录；不会启动从未尝试的日期 |

失败任务带有重试冷却时间，不会一直占据队列。近期更新时，即使某天重新请求失败，也会保留该日期此前已验证的数据，并记录刷新失败。

定时任务为北京时间 16:30、21:30 和次日 02:30：16:30 维护已经完成的日期，21:30 才尝试当日终版，02:30 推进历史。程序在北京时间 21:00 前最多只下载到昨天，避免将盘中数据当成终版；当天晚上仍未发布的数据保留为未解决。非交易日由实际交易日历跳过。GitHub 定时触发可能延迟，不等同于精准定时服务。

## 4. 数据在哪里，以及“完整”是什么意思

所有新结果写到 `data/v5/`，不改写旧 `data/daily/YYYY.parquet` 或旧状态文件：

- `data/v5/daily/2025/2025-01-03.parquet`：一个交易日一份数据。
- `data/v5/quality/2025-01-03.json`：覆盖、行数、校验和、市场宽度统计。
- `data/v5/state.json`：日期任务和验收门禁。
- `data/v5/calendar.json`：本次请求区间的完整交易日历。
- `data/v5/market_daily.parquet`：只根据验证通过的 V5 日期生成的宽度统计。
- `reports/v5/probe.json`：接口验收。
- `reports/v5/last_run.json`：实际新增日期、失败原因、总进度。
- `reports/v5/dates/`：缺少应有股票等部分返回的报告。

范围是 **BaoStock 历史名单内的沪深普通 A 股（包括主板、创业板、科创板；不包括 CDR、指数、北交所）**。覆盖检验的分母是当日源名单里正常交易的股票，不是现在的股票数量。停牌股票可以不返回日线，不纳入涨跌家数。该源名单本身是否遗漏退市证券不能单靠同源核对证明。

`source_universe_complete=true` 表示该来源历史名单内预期正常交易的股票都已收到；`all_china_a_complete=false` 始终保留，不把沪深源范围冒充沪深京全市场。返回缺少任何预期活跃股票时记录为 partial，不发布该新分片；旧的已验证分片继续保留。

原始价格为不复权，成交量统一口径为**股**，成交额为**人民币元**，换手率和涨跌幅为**百分数**（5 表示 5%）。probe 必须确认批量字段与标准个股字段一致。金额来自源真实字段；代码不使用收盘价乘成交量估算。不生成近似涨跌停、连板、炸板标签，避免用不完整规则误导回测。若接口缺少历史 `isST`，保留未知；不使用今天的名称倒推历史 ST。

## 5. 检查点和中断恢复

每完成一个日期，先原子写入 Parquet 和质量报告，再写状态。每 5 个日期或约 4 分钟进行一次 Git commit/push；最后再提交一次。网络挂起由独立子进程的父进程超时终止，重新建立连接。连续 5 个日期未解决时熔断本轮，Summary 列明原因。

接口空返回、权限错误、部分返回、日期错误都不会计为完成。脚本返回码 0 表示本轮无新问题，**不表示 2019 至今全部补齐**；查看 `verified_days / target_days`。返回码 2 表示验收/网络熔断/保存/推送等阻断问题，3 表示本轮有日期未解决；有效结果仍会尝试推送。

每轮还生成 `.recovery_v5/last-run.zip`，工作流将其作为 `v5-recovery-...` artifact 保存 7 天，包含本轮新保存的日期、质量记录、状态和报告。若 push 被拒绝，先下载该 artifact；将 zip 里的 `data/v5/`、`reports/v5/` 内容恢复到仓库相同路径后正常提交。旧工作流未暂停或手动编辑同一数据目录可能导致推送冲突；代码不会强推覆盖远端。

普通中断直接重跑即可。恢复时用 SHA-256 检查分片和质量记录；若上轮已经写数据但未更新状态，会恢复完成状态。数据损坏、缺失或状态 JSON 损坏会明确暴露，不默默覆盖。

## 6. 本地命令和验证

建议 Python 3.11。在仓库根目录运行：

```bash
python -m pip install -r requirements.txt
python sync_daily.py --mode probe
python sync_daily.py --mode recent --max-days 30
python sync_daily.py --mode history --max-days 100
python sync_daily.py --mode repair --max-days 30
python -m unittest discover -s tests -v
```

本地默认不自动提交 Git。仅在已设置正常 Git 身份/远端权限的仓库中使用 `--git-checkpoint`，GitHub 工作流已自动启用。

可选环境变量：`START_DATE`、`RUN_MINUTES`、`MAX_DAYS`、`RECENT_TRADING_DAYS`、`REQUEST_TIMEOUT`、`SOCKET_TIMEOUT`、`REQUEST_PAUSE`。保持 `RUN_MINUTES` 明显小于 job timeout，以便报告与推送有时间完成。

此文件包的离线测试检查缺失股票、错日期、价格异常、空返回、挂起进程、验收失败阻断以及多轮恢复。**离线测试通过不替代真实接口验收。** 文件生成环境尝试匿名连接 BaoStock 时返回网络接收错误；请以你实际 GitHub runner 的 `probe` 结果为准。
