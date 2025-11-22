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


def get_price(x):
    return x.units + (x.nano / 1e9)


def get_token() -> str:
    with open(os.path.join(os.path.expanduser('~'), 'no_commit', 'info.json')) as f:
        data = json.load(f)
    return data['token_tinkoff_sandbox']


async def request_iterator(instruments: list):
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


async def get_candles_with_limit(
        client, uid: str, to_time: datetime, limit: int, interval: CandleInterval
) -> pl.DataFrame:
    raw_candles = await client.market_data.get_candles(
        instrument_id=uid,
        to=to_time,
        limit=limit,
        interval=interval,
    )

    data_dicts = [
        {
            "high": get_price(x.high),
            "low": get_price(x.low),
            "close": get_price(x.close),
            "open": get_price(x.open),
            "volume": x.volume,
            "time": x.time.astimezone(TARGET_TZ),
        }
        for x in raw_candles.candles
    ]
    df = pl.DataFrame(data_dicts)
    return df


async def update_candles(candles: dict, in_queue: asyncio.Queue):
    while True:
        ticker, candle = await in_queue.get()
        candles[ticker] = pl.concat([candles[ticker], candle], how='vertical')

        print(f"Update for {ticker}")
        print(candles[ticker])


async def get_metadata(client, uid_list: list, out_queue: asyncio.Queue):
    instruments = [
        CandleInstrument(
            instrument_id=uid,
            interval=SubscriptionInterval.SUBSCRIPTION_INTERVAL_ONE_MINUTE,
        )
        for uid in uid_list
    ]
    async for metadata in client.market_data_stream.market_data_stream(
        request_iterator(instruments)
    ):
        await out_queue.put(metadata)


async def extract_candle(uid_to_ticker: dict, in_queue: asyncio.Queue, out_queue: asyncio.Queue):
    while True:
        metadata = await in_queue.get()
        try:
            df = pl.DataFrame(
                {
                "high": get_price(metadata.candle.high),
                "low": get_price(metadata.candle.low),
                "close": get_price(metadata.candle.close),
                "open": get_price(metadata.candle.open),
                "volume": metadata.candle.volume,
                "time": metadata.candle.time.astimezone(TARGET_TZ),
                }
            )
            ticker = uid_to_ticker[metadata.candle.instrument_uid]

            await out_queue.put((ticker, df))
        except AttributeError as e:
            print(f"AttributeError: {str(e)}")
            print({metadata})


async def main():
    api_token = get_token()
    async with AsyncSandboxClient(api_token) as client:
        shares = get_rub_shares(client)
        tickers = ['SBER', 'GAZP', 'T', 'LKOH', 'NVTK']
        uid_to_ticker = {shares[ticker]['uid']: ticker for ticker in tickers}

        ticker_candles = {
            ticker: await get_candles_with_limit(
                client, shares[ticker]['uid'], now(), 25, CandleInterval.CANDLE_INTERVAL_1_MIN
            ) for ticker in tickers
        }

        for key, value in ticker_candles.items():
            print(f"{key}: \n{value}")

        queue_metadata = asyncio.Queue()
        queue_candles = asyncio.Queue()
        await asyncio.gather(
            get_metadata(client, list(uid_to_ticker.keys()), queue_metadata),
            extract_candle(uid_to_ticker, queue_metadata, queue_candles),
            update_candles(ticker_candles, queue_candles)
        )


if __name__ == "__main__":
    asyncio.run(main())
