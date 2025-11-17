import os
import json
from datetime import datetime
from zoneinfo import ZoneInfo
import asyncio

import polars as pl

from tinkoff.invest.sandbox.async_client import AsyncSandboxClient
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


async def request_iterator(instruments: list, interval: SubscriptionInterval):
    yield MarketDataRequest(
        subscribe_candles_request=SubscribeCandlesRequest(
            waiting_close=True,
            subscription_action=SubscriptionAction.SUBSCRIPTION_ACTION_SUBSCRIBE,
            # candle_source_type=CandleSource.CANDLE_SOURCE_EXCHANGE,
            instruments=instruments,
        )
    )
    while True:
        await asyncio.sleep(1)


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


async def get_candles_with_limit(client, uid: str, to_time: datetime, limit: int,
                                 interval: CandleInterval) -> pl.DataFrame:
    raw_candles = await client.market_data.get_candles(
        instrument_id=uid,
        to=to_time,
        limit=limit,
        interval=interval,
    )

    def get_price(x):
        return x.units + (x.nano / NANO_DIVISOR)

    data_dicts = [
        {
            "high": get_price(x.high),
            "low": get_price(x.low),
            "close": get_price(x.close),
            "open": get_price(x.open),
            "time": x.time.astimezone(TARGET_TZ),
        }
        for x in raw_candles.candles
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


async def get_metadata(client, shares: dict, out_queue: asyncio.Queue):
    instruments = [
        CandleInstrument(
            instrument_id=uid,
            interval=SubscriptionInterval.SUBSCRIPTION_INTERVAL_ONE_MINUTE,
        )
        for uid in [shares['SBER']['uid'], shares['GAZP']['uid'], shares['T']['uid']]
    ]
    async for metadata in client.market_data_stream.market_data_stream(
        request_iterator(
            instruments, SubscriptionInterval.SUBSCRIPTION_INTERVAL_ONE_MINUTE
        )
    ):
        await out_queue.put(metadata)


async def print_metadata(in_queue: asyncio.Queue):
    candles = {}

    def get_price(x):
        return x.units + (x.nano / NANO_DIVISOR)

    while True:
        metadata = await in_queue.get()
        try:
            candle = {
                "high": get_price(metadata.candle.high),
                "low": get_price(metadata.candle.low),
                "close": get_price(metadata.candle.close),
                "open": get_price(metadata.candle.open),
                "volume": metadata.candle.volume,
                "time": metadata.candle.time.astimezone(TARGET_TZ),
            }
            key = metadata.candle.instrument_uid
            if key not in candles:
                candles[key] = []
            candles[key].append(candle)
            df = pl.DataFrame(candles[key])
            with pl.Config(tbl_rows=20):
                print(df.head(20))
        except AttributeError as e:
            print(f"AttributeError: {str(e)}")
            print({metadata})


async def main():
    api_token = get_token()
    async with AsyncSandboxClient(api_token) as client:
        shares = get_rub_shares(client)
        sber_candles = await get_candles_with_limit(
            client, shares['SBER']['uid'], now(), 25,CandleInterval.CANDLE_INTERVAL_1_MIN
        )

        print_candles(sber_candles)
        queue_metadata = asyncio.Queue()
        await asyncio.gather(
            get_metadata(client, shares, queue_metadata),
            print_metadata(queue_metadata)
        )


if __name__ == "__main__":
    asyncio.run(main())
