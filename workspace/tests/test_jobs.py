import tempfile
import unittest
from pathlib import Path

from app import db
from app.jobs import run_daily_job, sync_daily, sync_stock_basic


class FakeTushareClient:
    def __init__(self, stock_basic=None, daily_quotes=None, latest_trade_date="20240506"):
        self.stock_basic = stock_basic or []
        self.daily_quotes = daily_quotes or []
        self.latest_trade_date = latest_trade_date

    def get_stock_basic(self):
        return list(self.stock_basic)

    def get_daily_quotes(self, trade_date):
        _ = trade_date
        return list(self.daily_quotes)

    def get_latest_trade_date(self):
        return self.latest_trade_date


class JobTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.db_path = Path(self.tempdir.name) / "stock_data.db"
        db.init_db(self.db_path)

    def test_sync_stock_basic_writes_rows(self):
        client = FakeTushareClient(
            stock_basic=[
                {
                    "ts_code": "000001.SZ",
                    "symbol": "000001",
                    "name": "平安银行",
                    "area": "深圳",
                    "industry": "银行",
                    "market": "主板",
                    "list_status": "L",
                    "list_date": "19910403",
                    "delist_date": "",
                    "is_hs": "N",
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
                    "change": 0.2,
                    "pct_chg": 2.0,
                    "vol": 1000.0,
                    "amount": 10000.0,
                }
            ]
        )

        sync_daily(self.db_path, client, "20240506")
        sync_daily(self.db_path, client, "20240506")

        count = db.fetch_one_value(self.db_path, "SELECT COUNT(*) FROM daily_quotes")
        self.assertEqual(count, 1)

    def test_run_daily_job_records_task_runs(self):
        client = FakeTushareClient(
            stock_basic=[
                {
                    "ts_code": "000001.SZ",
                    "symbol": "000001",
                    "name": "平安银行",
                    "area": "深圳",
                    "industry": "银行",
                    "market": "主板",
                    "list_status": "L",
                    "list_date": "19910403",
                    "delist_date": "",
                    "is_hs": "N",
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
                    "change": 0.2,
                    "pct_chg": 2.0,
                    "vol": 1000.0,
                    "amount": 10000.0,
                }
            ],
            latest_trade_date="20240506",
        )

        run_daily_job(self.db_path, client)

        count = db.fetch_one_value(self.db_path, "SELECT COUNT(*) FROM sync_runs")
        self.assertEqual(count, 3)


if __name__ == "__main__":
    unittest.main()
