from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Dict, Iterable, Iterator, Optional


SCHEMA_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS stock_basic (
        ts_code TEXT PRIMARY KEY,
        symbol TEXT NOT NULL,
        name TEXT NOT NULL,
        area TEXT,
        industry TEXT,
        market TEXT,
        list_status TEXT,
        list_date TEXT,
        delist_date TEXT,
        is_hs TEXT,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS daily_quotes (
        ts_code TEXT NOT NULL,
        trade_date TEXT NOT NULL,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        pre_close REAL,
        change REAL,
        pct_chg REAL,
        vol REAL,
        amount REAL,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (ts_code, trade_date)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS sync_runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        task_name TEXT NOT NULL,
        status TEXT NOT NULL,
        details TEXT,
        started_at TEXT NOT NULL,
        finished_at TEXT NOT NULL
    )
    """,
]


@contextmanager
def connect(db_path: Path) -> Iterator[sqlite3.Connection]:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    try:
        yield connection
        connection.commit()
    finally:
        connection.close()


def init_db(db_path: Path) -> None:
    with connect(db_path) as connection:
        for statement in SCHEMA_STATEMENTS:
            connection.execute(statement)


def upsert_stock_basic(db_path: Path, rows: Iterable[Dict[str, object]]) -> int:
    rows_list = list(rows)
    if not rows_list:
        return 0

    sql = """
        INSERT INTO stock_basic (
            ts_code, symbol, name, area, industry, market,
            list_status, list_date, delist_date, is_hs, updated_at
        ) VALUES (
            :ts_code, :symbol, :name, :area, :industry, :market,
            :list_status, :list_date, :delist_date, :is_hs, CURRENT_TIMESTAMP
        )
        ON CONFLICT(ts_code) DO UPDATE SET
            symbol=excluded.symbol,
            name=excluded.name,
            area=excluded.area,
            industry=excluded.industry,
            market=excluded.market,
            list_status=excluded.list_status,
            list_date=excluded.list_date,
            delist_date=excluded.delist_date,
            is_hs=excluded.is_hs,
            updated_at=CURRENT_TIMESTAMP
    """

    with connect(db_path) as connection:
        connection.executemany(sql, rows_list)
    return len(rows_list)


def upsert_daily_quotes(db_path: Path, rows: Iterable[Dict[str, object]]) -> int:
    rows_list = list(rows)
    if not rows_list:
        return 0

    sql = """
        INSERT INTO daily_quotes (
            ts_code, trade_date, open, high, low, close, pre_close,
            change, pct_chg, vol, amount, updated_at
        ) VALUES (
            :ts_code, :trade_date, :open, :high, :low, :close, :pre_close,
            :change, :pct_chg, :vol, :amount, CURRENT_TIMESTAMP
        )
        ON CONFLICT(ts_code, trade_date) DO UPDATE SET
            open=excluded.open,
            high=excluded.high,
            low=excluded.low,
            close=excluded.close,
            pre_close=excluded.pre_close,
            change=excluded.change,
            pct_chg=excluded.pct_chg,
            vol=excluded.vol,
            amount=excluded.amount,
            updated_at=CURRENT_TIMESTAMP
    """

    with connect(db_path) as connection:
        connection.executemany(sql, rows_list)
    return len(rows_list)


def insert_sync_run(
    db_path: Path,
    task_name: str,
    status: str,
    started_at: str,
    finished_at: str,
    details: Optional[Dict[str, object]] = None,
) -> None:
    with connect(db_path) as connection:
        connection.execute(
            """
            INSERT INTO sync_runs (task_name, status, details, started_at, finished_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                task_name,
                status,
                None if details is None else json.dumps(details, ensure_ascii=False),
                started_at,
                finished_at,
            ),
        )


def fetch_one_value(db_path: Path, query: str) -> Optional[object]:
    with connect(db_path) as connection:
        row = connection.execute(query).fetchone()
    return None if row is None else row[0]
