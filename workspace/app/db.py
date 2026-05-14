from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Dict, Iterable, Iterator, Optional


SCHEMA_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS stock_basic (
        ts_code TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        industry TEXT,
        market TEXT,
        list_date TEXT,
        list_status TEXT,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS stock_daily (
        ts_code TEXT NOT NULL,
        trade_date TEXT NOT NULL,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        pre_close REAL,
        pct_chg REAL,
        vol REAL,
        amount REAL,
        turnover_rate REAL,
        volume_ratio REAL,
        pe_ttm REAL,
        pb REAL,
        total_mv REAL,
        circ_mv REAL,
        adj_factor REAL,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (ts_code, trade_date)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS stock_finance (
        ts_code TEXT NOT NULL,
        ann_date TEXT,
        end_date TEXT NOT NULL,
        roe REAL,
        grossprofit_margin REAL,
        netprofit_margin REAL,
        tr_yoy REAL,
        netprofit_yoy REAL,
        dt_netprofit_yoy REAL,
        debt_to_assets REAL,
        ocf_to_or REAL,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (ts_code, end_date)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS sync_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        api_name TEXT NOT NULL,
        start_date TEXT,
        end_date TEXT,
        status TEXT NOT NULL,
        row_count INTEGER DEFAULT 0,
        error_msg TEXT,
        started_at TEXT,
        finished_at TEXT
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


def _project_rows(rows: Iterable[Dict[str, object]], fields: list[str]) -> list[Dict[str, object]]:
    return [{field: row.get(field) for field in fields} for row in rows]


def upsert_stock_basic(db_path: Path, rows: Iterable[Dict[str, object]]) -> int:
    fields = ["ts_code", "name", "industry", "market", "list_date", "list_status"]
    rows_list = _project_rows(rows, fields)
    if not rows_list:
        return 0

    sql = """
        INSERT INTO stock_basic (
            ts_code, name, industry, market, list_date, list_status, updated_at
        ) VALUES (
            :ts_code, :name, :industry, :market, :list_date, :list_status, CURRENT_TIMESTAMP
        )
        ON CONFLICT(ts_code) DO UPDATE SET
            name=excluded.name,
            industry=excluded.industry,
            market=excluded.market,
            list_date=excluded.list_date,
            list_status=excluded.list_status,
            updated_at=CURRENT_TIMESTAMP
    """

    with connect(db_path) as connection:
        connection.executemany(sql, rows_list)
    return len(rows_list)


def upsert_stock_daily(db_path: Path, rows: Iterable[Dict[str, object]]) -> int:
    fields = [
        "ts_code",
        "trade_date",
        "open",
        "high",
        "low",
        "close",
        "pre_close",
        "pct_chg",
        "vol",
        "amount",
        "turnover_rate",
        "volume_ratio",
        "pe_ttm",
        "pb",
        "total_mv",
        "circ_mv",
        "adj_factor",
    ]
    rows_list = _project_rows(rows, fields)
    if not rows_list:
        return 0

    sql = """
        INSERT INTO stock_daily (
            ts_code, trade_date, open, high, low, close, pre_close,
            pct_chg, vol, amount, turnover_rate, volume_ratio, pe_ttm,
            pb, total_mv, circ_mv, adj_factor, updated_at
        ) VALUES (
            :ts_code, :trade_date, :open, :high, :low, :close, :pre_close,
            :pct_chg, :vol, :amount, :turnover_rate, :volume_ratio, :pe_ttm,
            :pb, :total_mv, :circ_mv, :adj_factor, CURRENT_TIMESTAMP
        )
        ON CONFLICT(ts_code, trade_date) DO UPDATE SET
            open=excluded.open,
            high=excluded.high,
            low=excluded.low,
            close=excluded.close,
            pre_close=excluded.pre_close,
            pct_chg=excluded.pct_chg,
            vol=excluded.vol,
            amount=excluded.amount,
            turnover_rate=excluded.turnover_rate,
            volume_ratio=excluded.volume_ratio,
            pe_ttm=excluded.pe_ttm,
            pb=excluded.pb,
            total_mv=excluded.total_mv,
            circ_mv=excluded.circ_mv,
            adj_factor=excluded.adj_factor,
            updated_at=CURRENT_TIMESTAMP
    """

    with connect(db_path) as connection:
        connection.executemany(sql, rows_list)
    return len(rows_list)


def upsert_stock_finance(db_path: Path, rows: Iterable[Dict[str, object]]) -> int:
    fields = [
        "ts_code",
        "ann_date",
        "end_date",
        "roe",
        "grossprofit_margin",
        "netprofit_margin",
        "tr_yoy",
        "netprofit_yoy",
        "dt_netprofit_yoy",
        "debt_to_assets",
        "ocf_to_or",
    ]
    rows_list = _project_rows(rows, fields)
    if not rows_list:
        return 0

    sql = """
        INSERT INTO stock_finance (
            ts_code, ann_date, end_date, roe, grossprofit_margin,
            netprofit_margin, tr_yoy, netprofit_yoy, dt_netprofit_yoy,
            debt_to_assets, ocf_to_or, updated_at
        ) VALUES (
            :ts_code, :ann_date, :end_date, :roe, :grossprofit_margin,
            :netprofit_margin, :tr_yoy, :netprofit_yoy, :dt_netprofit_yoy,
            :debt_to_assets, :ocf_to_or, CURRENT_TIMESTAMP
        )
        ON CONFLICT(ts_code, end_date) DO UPDATE SET
            ann_date=excluded.ann_date,
            roe=excluded.roe,
            grossprofit_margin=excluded.grossprofit_margin,
            netprofit_margin=excluded.netprofit_margin,
            tr_yoy=excluded.tr_yoy,
            netprofit_yoy=excluded.netprofit_yoy,
            dt_netprofit_yoy=excluded.dt_netprofit_yoy,
            debt_to_assets=excluded.debt_to_assets,
            ocf_to_or=excluded.ocf_to_or,
            updated_at=CURRENT_TIMESTAMP
    """

    with connect(db_path) as connection:
        connection.executemany(sql, rows_list)
    return len(rows_list)


def insert_sync_log(
    db_path: Path,
    api_name: str,
    status: str,
    started_at: Optional[str] = None,
    finished_at: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    row_count: int = 0,
    error_msg: Optional[str] = None,
) -> None:
    with connect(db_path) as connection:
        connection.execute(
            """
            INSERT INTO sync_log (
                api_name, start_date, end_date, status, row_count,
                error_msg, started_at, finished_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                api_name,
                start_date,
                end_date,
                status,
                row_count,
                error_msg,
                started_at,
                finished_at,
            ),
        )


def fetch_one_value(db_path: Path, query: str) -> Optional[object]:
    with connect(db_path) as connection:
        row = connection.execute(query).fetchone()
    return None if row is None else row[0]
