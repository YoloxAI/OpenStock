# OpenClaw 股票分析系统

这是一个 OpenClaw 工作区，第一版聚焦四类核心能力：

- 同步 A 股股票清单
- 同步指定交易日的行情、估值、流动性、市值和复权因子
- 同步核心财务指标
- 记录采集日志，便于排查问题

项目仅用于技术学习和投资研究辅助，不构成投资建议。

## 目录

```text
.
├── .env.example
└── workspace/
    ├── app/
    ├── data/
    └── tests/
```

## 配置

根目录 `.env` 示例：

```env
TUSHARE_TOKEN=your_tushare_token_here
DB_PATH=data/stock_data.db
```

`DB_PATH` 是相对 `workspace/` 的路径。

## 运行

```bash
cd workspace
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m app.cli init-db
python -m app.cli sync-basic
python -m app.cli sync-daily --date 20240506
python -m app.cli sync-finance --period 20240331
```

## 数据表

```text
stock_data.db
├── stock_basic     股票基础信息
├── stock_daily     每日行情 + 估值 + 复权因子
├── stock_finance   核心财务指标
└── sync_log        采集日志
```

`sync-daily` 会将 Tushare 的 `daily`、`daily_basic`、`adj_factor`
按 `ts_code + trade_date` 合并写入 `stock_daily`。

## 命令

- `python -m app.cli init-db`
- `python -m app.cli sync-basic`
- `python -m app.cli sync-daily --date YYYYMMDD`
- `python -m app.cli sync-finance --period YYYYMMDD`
- `python -m app.cli sync-daily-latest`
- `python -m app.cli run-daily-job`
