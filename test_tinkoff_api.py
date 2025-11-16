import os
import json
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import polars as pl

from tinkoff.invest.sandbox.client import SandboxClient
from tinkoff.invest import (
    CandleInterval,
    MarketDataRequest,
    SubscribeCandlesRequest,
    SubscriptionAction,
    SubscriptionInterval,
    CandleInstrument,
)
from tinkoff.invest.utils import now


DATA_DIR_PATH = os.path.join(os.curdir, "data")
SHARES_FILE_PATH = os.path.join(DATA_DIR_PATH, "shares.json")

TARGET_TZ = ZoneInfo("Europe/Moscow")
NANO_DIVISOR = 1_000_000_000


def get_token() -> str:
    with open(os.path.join(os.path.expanduser('~'), 'no_commit', 'info.json')) as f:
        data = json.load(f)
    return data['token_tinkoff_sandbox']


def request_iterator(uid: str, interval: SubscriptionInterval):
    yield MarketDataRequest(
        subscribe_candles_request=SubscribeCandlesRequest(
            waiting_close=True,
            subscription_action=SubscriptionAction.SUBSCRIPTION_ACTION_SUBSCRIBE,
            # candle_source_type=CandleSource.CANDLE_SOURCE_EXCHANGE,
            instruments=[
                CandleInstrument(
                    instrument_id=uid,
                    interval=interval,
                )
            ],
        )
    )
    while True:
        time.sleep(1)


def get_rub_shares(client) -> dict:
    if not os.path.isdir(DATA_DIR_PATH):
        os.mkdir(DATA_DIR_PATH)
    if not os.path.isfile(SHARES_FILE_PATH):
        rub_shares = {
            x.ticker: {
                'figi': x.figi,
                'uid': x.uid,
                'class_code': x.class_code,
                'name': x.name,
                'isin': x.isin
            }
            for x in client.instruments.shares().instruments if x.currency == "rub"
        }
        with open(SHARES_FILE_PATH, 'w') as f:
            json.dump(rub_shares, f)
    with open(SHARES_FILE_PATH, 'r') as f:
        return json.load(f)


def get_candles_with_limit(client, uid: str, to_time: datetime, limit: int,
                           interval: CandleInterval) -> pl.DataFrame:
    raw_candles = client.market_data.get_candles(
        instrument_id=uid,
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
        sber_candles = get_candles_with_limit(
            client, shares['SBER']['uid'], now(), 25,CandleInterval.CANDLE_INTERVAL_1_MIN
        )
        print_candles(sber_candles)
        for metadata in client.market_data_stream.market_data_stream(
            request_iterator(shares['SBER']['uid'], SubscriptionInterval.SUBSCRIPTION_INTERVAL_ONE_MINUTE)
        ):
            print(metadata)


if __name__ == "__main__":
    main()
