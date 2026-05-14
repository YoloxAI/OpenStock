from __future__ import annotations

from datetime import datetime, timedelta
from typing import Dict, List


class TushareClientError(RuntimeError):
    """Raised when the Tushare client cannot run correctly."""


class TushareClient:
    def __init__(self, token: str):
        self.token = token
        self._pro = self._build_client()

    def _build_client(self):
        try:
            import tushare as ts
        except ImportError as exc:
            raise TushareClientError(
                "tushare is not installed. Run `pip install -r requirements.txt`."
            ) from exc

        return ts.pro_api(token=self.token)

    @staticmethod
    def _frame_to_records(frame) -> List[Dict[str, object]]:
        if frame is None or frame.empty:
            return []
        return frame.fillna("").to_dict(orient="records")

    def get_stock_basic(self) -> List[Dict[str, object]]:
        frame = self._pro.stock_basic(
            exchange="",
            list_status="L",
            fields="ts_code,name,industry,market,list_date,list_status",
        )
        return self._frame_to_records(frame)

    def get_daily_quotes(self, trade_date: str) -> List[Dict[str, object]]:
        frame = self._pro.daily(
            trade_date=trade_date,
            fields="ts_code,trade_date,open,high,low,close,pre_close,pct_chg,vol,amount",
        )
        return self._frame_to_records(frame)

    def get_daily_basic(self, trade_date: str) -> List[Dict[str, object]]:
        frame = self._pro.daily_basic(
            trade_date=trade_date,
            fields=(
                "ts_code,trade_date,turnover_rate,volume_ratio,"
                "pe_ttm,pb,total_mv,circ_mv"
            ),
        )
        return self._frame_to_records(frame)

    def get_adj_factor(self, trade_date: str) -> List[Dict[str, object]]:
        frame = self._pro.adj_factor(
            trade_date=trade_date,
            fields="ts_code,trade_date,adj_factor",
        )
        return self._frame_to_records(frame)

    def get_fina_indicator(self, period: str) -> List[Dict[str, object]]:
        frame = self._pro.fina_indicator(
            period=period,
            fields=(
                "ts_code,ann_date,end_date,roe,grossprofit_margin,"
                "netprofit_margin,tr_yoy,netprofit_yoy,dt_netprofit_yoy,"
                "debt_to_assets,ocf_to_or"
            ),
        )
        return self._frame_to_records(frame)

    def get_latest_trade_date(self) -> str:
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=30)).strftime("%Y%m%d")
        frame = self._pro.trade_cal(
            exchange="SSE",
            start_date=start_date,
            end_date=end_date,
            is_open="1",
            fields="cal_date",
        )
        records = self._frame_to_records(frame)
        if not records:
            raise TushareClientError("No trading calendar data returned from Tushare.")
        return str(records[-1]["cal_date"])
