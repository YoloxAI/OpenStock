from __future__ import annotations

import argparse
import sys
from typing import Optional

from .config import ConfigError, load_settings
from .db import init_db
from .jobs import run_daily_job, sync_daily, sync_daily_latest, sync_finance, sync_stock_basic
from .tushare_client import TushareClient, TushareClientError


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="OpenClaw A-share data collector")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("init-db", help="Initialize SQLite tables")
    subparsers.add_parser("sync-basic", help="Sync stock basic data")

    sync_daily_parser = subparsers.add_parser("sync-daily", help="Sync daily quotes by date")
    sync_daily_parser.add_argument("--date", required=True, help="Trade date in YYYYMMDD")

    sync_finance_parser = subparsers.add_parser("sync-finance", help="Sync finance indicators by period")
    sync_finance_parser.add_argument("--period", required=True, help="Report period in YYYYMMDD")

    subparsers.add_parser("sync-daily-latest", help="Sync daily quotes for latest trade date")
    subparsers.add_parser("run-daily-job", help="Run stock basic sync and latest daily sync")

    return parser


def _validate_trade_date(value: str) -> str:
    if len(value) != 8 or not value.isdigit():
        raise ValueError("Date value must use YYYYMMDD format.")
    return value


def main(argv: Optional[list[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        settings = load_settings(require_token=args.command != "init-db")
    except ConfigError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2

    if args.command == "init-db":
        init_db(settings.db_path)
        print(f"Initialized database at {settings.db_path}")
        return 0

    try:
        client = TushareClient(settings.tushare_token)

        if args.command == "sync-basic":
            result = sync_stock_basic(settings.db_path, client)
        elif args.command == "sync-daily":
            result = sync_daily(settings.db_path, client, _validate_trade_date(args.date))
        elif args.command == "sync-finance":
            result = sync_finance(settings.db_path, client, _validate_trade_date(args.period))
        elif args.command == "sync-daily-latest":
            result = sync_daily_latest(settings.db_path, client)
        elif args.command == "run-daily-job":
            result = run_daily_job(settings.db_path, client)
        else:
            parser.error(f"Unknown command: {args.command}")
            return 2
    except (TushareClientError, RuntimeError, ValueError) as exc:
        print(f"Runtime error: {exc}", file=sys.stderr)
        return 1

    print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
