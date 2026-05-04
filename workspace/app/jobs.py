from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Dict

from . import db


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _save_result(db_path: Path, task_name: str, result: Dict[str, object], started_at: str) -> Dict[str, object]:
    finished_at = _utc_now()
    db.insert_sync_run(
        db_path=db_path,
        task_name=task_name,
        status="success",
        details=result,
        started_at=started_at,
        finished_at=finished_at,
    )
    return result


def _save_error(db_path: Path, task_name: str, error: Exception, started_at: str) -> None:
    finished_at = _utc_now()
    db.insert_sync_run(
        db_path=db_path,
        task_name=task_name,
        status="failed",
        details={"error": str(error)},
        started_at=started_at,
        finished_at=finished_at,
    )


def sync_stock_basic(db_path: Path, client) -> Dict[str, object]:
    started_at = _utc_now()
    try:
        rows = client.get_stock_basic()
        result = {"rows": db.upsert_stock_basic(db_path, rows)}
        return _save_result(db_path, "sync_stock_basic", result, started_at)
    except Exception as exc:
        _save_error(db_path, "sync_stock_basic", exc, started_at)
        raise


def sync_daily(db_path: Path, client, trade_date: str) -> Dict[str, object]:
    started_at = _utc_now()
    try:
        rows = client.get_daily_quotes(trade_date)
        result = {"trade_date": trade_date, "rows": db.upsert_daily_quotes(db_path, rows)}
        return _save_result(db_path, "sync_daily", result, started_at)
    except Exception as exc:
        _save_error(db_path, "sync_daily", exc, started_at)
        raise


def sync_daily_latest(db_path: Path, client) -> Dict[str, object]:
    latest_trade_date = client.get_latest_trade_date()
    result = sync_daily(db_path, client, latest_trade_date)
    return {"trade_date": latest_trade_date, **result}


def run_daily_job(db_path: Path, client) -> Dict[str, object]:
    started_at = _utc_now()
    try:
        stock_basic_result = sync_stock_basic(db_path, client)
        latest_trade_date = client.get_latest_trade_date()
        daily_result = sync_daily(db_path, client, latest_trade_date)
        result = {
            "stock_basic": stock_basic_result,
            "daily": daily_result,
        }
        return _save_result(db_path, "run_daily_job", result, started_at)
    except Exception as exc:
        _save_error(db_path, "run_daily_job", exc, started_at)
        raise
