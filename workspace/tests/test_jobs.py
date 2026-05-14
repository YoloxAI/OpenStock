import tempfile
import unittest
from pathlib import Path

from app import db
from app.jobs import run_daily_job, sync_daily, sync_finance, sync_stock_basic


class FakeTushareClient:
    def __init__(
        self,
        stock_basic=None,
        daily_quotes=None,
        daily_basic=None,
        adj_factor=None,
        fina_indicator=None,
        latest_trade_date="20240506",
        fail_daily_basic=False,
    ):
        self.stock_basic = stock_basic or []
        self.daily_quotes = daily_quotes or []
        self.daily_basic = daily_basic or []
        self.adj_factor = adj_factor or []
        self.fina_indicator = fina_indicator or []
        self.latest_trade_date = latest_trade_date
        self.fail_daily_basic = fail_daily_basic

    def get_stock_basic(self):
        return list(self.stock_basic)

    def get_daily_quotes(self, trade_date):
        _ = trade_date
        return list(self.daily_quotes)

    def get_daily_basic(self, trade_date):
        _ = trade_date
        if self.fail_daily_basic:
            raise RuntimeError("daily_basic failed")
        return list(self.daily_basic)

    def get_adj_factor(self, trade_date):
        _ = trade_date
        return list(self.adj_factor)

    def get_fina_indicator(self, period):
        _ = period
        return list(self.fina_indicator)

    def get_latest_trade_date(self):
        return self.latest_trade_date


class JobTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.db_path = Path(self.tempdir.name) / "stock_data.db"
        db.init_db(self.db_path)

    def test_init_db_creates_core_tables(self):
        with db.connect(self.db_path) as connection:
            rows = connection.execute(
                """
                SELECT name FROM sqlite_master
                WHERE type='table' AND name IN (
                    'stock_basic', 'stock_daily', 'stock_finance', 'sync_log'
                )
                ORDER BY name
                """
            ).fetchall()

        self.assertEqual(
            [row["name"] for row in rows],
            ["stock_basic", "stock_daily", "stock_finance", "sync_log"],
        )

    def test_sync_stock_basic_writes_rows(self):
        client = FakeTushareClient(
            stock_basic=[
                {
                    "ts_code": "000001.SZ",
                    "name": "平安银行",
                    "industry": "银行",
                    "market": "主板",
                    "list_status": "L",
                    "list_date": "19910403",
                }
            ]
        )

        result = sync_stock_basic(self.db_path, client)

        count = db.fetch_one_value(self.db_path, "SELECT COUNT(*) FROM stock_basic")
        self.assertEqual(result["rows"], 1)
        self.assertEqual(count, 1)

    def test_sync_daily_is_idempotent(self):
        client = FakeTushareClient(
            daily_quotes=[
                {
                    "ts_code": "000001.SZ",
                    "trade_date": "20240506",
                    "open": 10.0,
                    "high": 10.5,
                    "low": 9.8,
                    "close": 10.2,
                    "pre_close": 10.0,
                    "pct_chg": 2.0,
                    "vol": 1000.0,
                    "amount": 10000.0,
                }
            ],
            daily_basic=[
                {
                    "ts_code": "000001.SZ",
                    "trade_date": "20240506",
                    "turnover_rate": 1.2,
                    "volume_ratio": 0.8,
                    "pe_ttm": 5.5,
                    "pb": 0.7,
                    "total_mv": 100000.0,
                    "circ_mv": 80000.0,
                }
            ],
            adj_factor=[
                {
                    "ts_code": "000001.SZ",
                    "trade_date": "20240506",
                    "adj_factor": 123.45,
                }
            ]
        )

        sync_daily(self.db_path, client, "20240506")
        sync_daily(self.db_path, client, "20240506")

        count = db.fetch_one_value(self.db_path, "SELECT COUNT(*) FROM stock_daily")
        pe_ttm = db.fetch_one_value(
            self.db_path,
            "SELECT pe_ttm FROM stock_daily WHERE ts_code='000001.SZ' AND trade_date='20240506'",
        )
        adj_factor = db.fetch_one_value(
            self.db_path,
            "SELECT adj_factor FROM stock_daily WHERE ts_code='000001.SZ' AND trade_date='20240506'",
        )
        self.assertEqual(count, 1)
        self.assertEqual(pe_ttm, 5.5)
        self.assertEqual(adj_factor, 123.45)

    def test_sync_finance_is_idempotent(self):
        client = FakeTushareClient(
            fina_indicator=[
                {
                    "ts_code": "000001.SZ",
                    "ann_date": "20240420",
                    "end_date": "20240331",
                    "roe": 3.2,
                    "grossprofit_margin": 45.0,
                    "netprofit_margin": 20.0,
                    "tr_yoy": 12.3,
                    "netprofit_yoy": 8.9,
                    "dt_netprofit_yoy": 7.6,
                    "debt_to_assets": 55.0,
                    "ocf_to_or": 0.9,
                }
            ]
        )

        sync_finance(self.db_path, client, "20240331")
        sync_finance(self.db_path, client, "20240331")

        count = db.fetch_one_value(self.db_path, "SELECT COUNT(*) FROM stock_finance")
        roe = db.fetch_one_value(
            self.db_path,
            "SELECT roe FROM stock_finance WHERE ts_code='000001.SZ' AND end_date='20240331'",
        )
        self.assertEqual(count, 1)
        self.assertEqual(roe, 3.2)

    def test_run_daily_job_records_task_runs(self):
        client = FakeTushareClient(
            stock_basic=[
                {
                    "ts_code": "000001.SZ",
                    "name": "平安银行",
                    "industry": "银行",
                    "market": "主板",
                    "list_status": "L",
                    "list_date": "19910403",
                }
            ],
            daily_quotes=[
                {
                    "ts_code": "000001.SZ",
                    "trade_date": "20240506",
                    "open": 10.0,
                    "high": 10.5,
                    "low": 9.8,
                    "close": 10.2,
                    "pre_close": 10.0,
                    "pct_chg": 2.0,
                    "vol": 1000.0,
                    "amount": 10000.0,
                }
            ],
            daily_basic=[],
            adj_factor=[],
            latest_trade_date="20240506",
        )

        run_daily_job(self.db_path, client)

        count = db.fetch_one_value(self.db_path, "SELECT COUNT(*) FROM sync_log")
        self.assertEqual(count, 3)

    def test_failed_sync_daily_records_error(self):
        client = FakeTushareClient(fail_daily_basic=True)

        with self.assertRaises(RuntimeError):
            sync_daily(self.db_path, client, "20240506")

        status = db.fetch_one_value(self.db_path, "SELECT status FROM sync_log")
        error_msg = db.fetch_one_value(self.db_path, "SELECT error_msg FROM sync_log")
        self.assertEqual(status, "failed")
        self.assertEqual(error_msg, "daily_basic failed")


if __name__ == "__main__":
    unittest.main()
