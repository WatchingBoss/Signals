import polars as pl
from datetime import datetime
from zoneinfo import ZoneInfo


TARGET_TZ = ZoneInfo("Europe/Moscow")


class AlertEngine:
    def __init__(self):
        self.current_trading_day = None
        self.daily_open: dict[str, float] = {}
        self.alert_1_percent_open = set()
        self.last_1m_alert: dict[str, datetime] = {}


    def analyze(self, df: pl.DataFrame) -> list[dict]:
        recent_df = df.group_by("uid").tail(16).sort(["uid", "timestamp_utc"])

        latest_candle = recent_df.group_by("uid").last()

        last_1m_alert_list: list[dict] = []

        for row in latest_candle.iter_rows(named=True):
            uid = row["uid"]
            ticker = row["ticker"]
            name = row["name"]
            candle_open = row["open"]
            candle_close = row["close"]
            timestamp_utc = row["timestamp_utc"]

            if uid in self.last_1m_alert:
                if timestamp_utc <= self.last_1m_alert[uid]:
                    continue
            change_1m = abs(candle_close - candle_open) / candle_open
            if change_1m > 0.002:
                self.last_1m_alert[uid] = timestamp_utc
                if candle_open > candle_close:
                    continue
                alert_data = {
                    "ticker": ticker,
                    "name": name,
                    "direction": "РОСТ",
                    "change_percent": round(change_1m * 100, 2),
                    "candle_close": candle_close,
                    "timestamp_utc": timestamp_utc.astimezone(TARGET_TZ).isoformat()
                }
                last_1m_alert_list.append(alert_data)

        return last_1m_alert_list
