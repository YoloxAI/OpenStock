from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, Optional, Tuple

from . import db


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _save_result(
    db_path: Path,
    api_name: str,
    result: Dict[str, object],
    started_at: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    row_count: int = 0,
) -> Dict[str, object]:
    finished_at = _utc_now()
    db.insert_sync_log(
        db_path=db_path,
        api_name=api_name,
        status="success",
        started_at=started_at,
        finished_at=finished_at,
        start_date=start_date,
        end_date=end_date,
        row_count=row_count,
    )
    return result


def _save_error(
    db_path: Path,
    api_name: str,
    error: Exception,
    started_at: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> None:
    finished_at = _utc_now()
    db.insert_sync_log(
        db_path=db_path,
        api_name=api_name,
        status="failed",
        started_at=started_at,
        finished_at=finished_at,
        start_date=start_date,
        end_date=end_date,
        error_msg=str(error),
    )


def _row_key(row: Dict[str, object]) -> Tuple[str, str]:
    return (str(row["ts_code"]), str(row["trade_date"]))


def _merge_daily_rows(*row_groups: Iterable[Dict[str, object]]) -> list[Dict[str, object]]:
    merged: dict[Tuple[str, str], Dict[str, object]] = {}
    for rows in row_groups:
        for row in rows:
            key = _row_key(row)
            merged.setdefault(key, {"ts_code": key[0], "trade_date": key[1]}).update(row)
    return list(merged.values())


def sync_stock_basic(db_path: Path, client) -> Dict[str, object]:
    started_at = _utc_now()
    try:
        rows = client.get_stock_basic()
        row_count = db.upsert_stock_basic(db_path, rows)
        result = {"rows": row_count}
        return _save_result(db_path, "stock_basic", result, started_at, row_count=row_count)
    except Exception as exc:
        _save_error(db_path, "stock_basic", exc, started_at)
        raise


def sync_daily(db_path: Path, client, trade_date: str) -> Dict[str, object]:
    started_at = _utc_now()
    try:
        daily_rows = client.get_daily_quotes(trade_date)
        daily_basic_rows = client.get_daily_basic(trade_date)
        adj_factor_rows = client.get_adj_factor(trade_date)
        merged_rows = _merge_daily_rows(daily_rows, daily_basic_rows, adj_factor_rows)
        row_count = db.upsert_stock_daily(db_path, merged_rows)
        result = {
            "trade_date": trade_date,
            "rows": row_count,
            "source_rows": {
                "daily": len(daily_rows),
                "daily_basic": len(daily_basic_rows),
                "adj_factor": len(adj_factor_rows),
            },
        }
        return _save_result(
            db_path,
            "stock_daily",
            result,
            started_at,
            start_date=trade_date,
            end_date=trade_date,
            row_count=row_count,
        )
    except Exception as exc:
        _save_error(db_path, "stock_daily", exc, started_at, start_date=trade_date, end_date=trade_date)
        raise


def sync_finance(db_path: Path, client, period: str) -> Dict[str, object]:
    started_at = _utc_now()
    try:
        rows = client.get_fina_indicator(period)
        row_count = db.upsert_stock_finance(db_path, rows)
        result = {"period": period, "rows": row_count}
        return _save_result(
            db_path,
            "stock_finance",
            result,
            started_at,
            start_date=period,
            end_date=period,
            row_count=row_count,
        )
    except Exception as exc:
        _save_error(db_path, "stock_finance", exc, started_at, start_date=period, end_date=period)
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
        return _save_result(db_path, "run_daily_job", result, started_at, row_count=0)
    except Exception as exc:
        _save_error(db_path, "run_daily_job", exc, started_at)
        raise
