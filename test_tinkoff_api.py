import os
import json
from datetime import datetime
from zoneinfo import ZoneInfo

import polars as pl

from tinkoff.invest.sandbox.client import SandboxClient
from tinkoff.invest import CandleInterval
from tinkoff.invest.utils import now


DATA_DIR_PATH = os.path.join(os.curdir, "data")
SHARES_FILE_PATH = os.path.join(DATA_DIR_PATH, "shares.json")

TARGET_TZ = ZoneInfo("Europe/Moscow")
NANO_DIVISOR = 1_000_000_000


def get_token() -> str:
    with open(os.path.join(os.path.expanduser('~'), 'no_commit', 'info.json')) as f:
        data = json.load(f)
    return data['token_tinkoff_sandbox']


def get_rub_shares(client) -> dict:
    if not os.path.isdir(DATA_DIR_PATH):
        os.mkdir(DATA_DIR_PATH)
    if not os.path.isfile(SHARES_FILE_PATH):
        rub_shares = {x.ticker: (x.figi, x.class_code, x.name)
                      for x in client.instruments.shares().instruments if x.currency == "rub"}
        with open(SHARES_FILE_PATH, 'w') as f:
            json.dump(rub_shares, f)
    with open(SHARES_FILE_PATH, 'r') as f:
        return json.load(f)


def get_candles_with_limit(client, figi: str, to_time: datetime, limit: int,
                           interval: CandleInterval) -> pl.DataFrame:
    raw_candles = client.market_data.get_candles(
        instrument_id=figi,
        to=to_time,
        limit=limit,
        interval=interval,
    ).candles
    def get_price(x): return x.units + (x.nano / NANO_DIVISOR)
    data_dicts = [
        {
            "high": get_price(x.high),
            "low": get_price(x.low),
            "close": get_price(x.close),
            "open": get_price(x.open),
            "time": x.time.astimezone(TARGET_TZ),
        }
        for x in raw_candles
    ]
    df = pl.DataFrame(data_dicts)
    df = df.with_columns(pl.col('time').dt.strftime("%H:%M:%S").alias("time_string"))
    return df


def print_candles(df: pl.DataFrame, rows: int = 20):
    with pl.Config(tbl_rows=rows):
        print(
            df.head(20).select(
                ["high", "low", "close", "open", "time_string"]
            )
        )


def main():
    api_token = get_token()
    with SandboxClient(api_token) as client:
        shares = get_rub_shares(client)
        sber_candles = get_candles_with_limit(client, shares['SBER'][0], now(), 50,
                                              CandleInterval.CANDLE_INTERVAL_5_SEC)
        print_candles(sber_candles)


if __name__ == "__main__":
    main()
