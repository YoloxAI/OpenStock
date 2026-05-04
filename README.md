# OpenClaw 股票分析系统

这是一个 OpenClaw 工作区，目前只做两件事：

- 同步 A 股股票清单
- 同步指定交易日的日线行情

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
```

## 命令

- `python -m app.cli init-db`
- `python -m app.cli sync-basic`
- `python -m app.cli sync-daily --date YYYYMMDD`
- `python -m app.cli sync-daily-latest`
- `python -m app.cli run-daily-job`
