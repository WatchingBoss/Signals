from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import asyncio

import polars as pl

from t_tech.invest import AsyncClient
from t_tech.invest import (
    CandleInterval,
    MarketDataRequest,
    SubscribeCandlesRequest,
    SubscriptionAction,
    SubscriptionInterval,
    CandleInstrument,
)
from t_tech.invest.exceptions import (
    AioRequestError,
)
from t_tech.invest.utils import now

from config import INVEST_TOKEN
from alert_engine import AlertEngine
from rabbitmq import RabbitMQPublisher


TARGET_TZ = ZoneInfo("Europe/Moscow")

SCHEMA = {
    "ticker": pl.Utf8,
    "uid": pl.Utf8,
    "name": pl.Utf8,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "volume": pl.Int64,
    "timestamp_utc": pl.Datetime(time_unit="us", time_zone="UTC")
}


def quotation_to_float(x):
    return x.units + (x.nano / 1e9)


async def get_rub_shares(client) -> dict:
    shares = await client.instruments.shares()
    rub_shares = {
        x.uid: {
            'ticker': x.ticker,
            'name': x.name,
        }
        for x in shares.instruments if x.currency == "rub"
    }
    return rub_shares


async def fetch_candles_for_share(client, to: datetime, from_: datetime,
                                  ticker: str, uid: str, name: str,
                                  sem: asyncio.Semaphore) -> list[dict]:
    async with sem:
        try:
            raw_candles = await client.market_data.get_candles(
                instrument_id=uid,
                to=to,
                from_=from_,
                interval=CandleInterval.CANDLE_INTERVAL_1_MIN
            )
            return [
                {
                   "ticker": ticker,
                    "uid": uid,
                    "name": name,
                    "open": quotation_to_float(c.open),
                    "high": quotation_to_float(c.high),
                    "low": quotation_to_float(c.low),
                    "close": quotation_to_float(c.close),
                    "volume": c.volume,
                    "timestamp_utc": c.time
                }
                for c in raw_candles.candles
            ]

        except Exception as e:
            print(f"Error fetching candles for {ticker}: {e}")
            return []


async def minute_candles_to_dataframe(client, shares: dict) -> pl.DataFrame:
    sem = asyncio.Semaphore(10)

    tasks = [
        fetch_candles_for_share(
            client=client,
            to=now(),
            from_=now() - timedelta(days=1),
            ticker=info["ticker"],
            uid=uid,
            name=info["name"],
            sem=sem
        )
        for uid, info in shares.items()
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)

    all_rows = []
    for r in results:
        if isinstance(r, Exception):
            continue
        all_rows.extend(r)

    df = pl.DataFrame(all_rows, schema=SCHEMA)
    df = df.sort(["timestamp_utc", "ticker"])

    return df


async def request_iterator(instruments: list):
    yield MarketDataRequest(
        subscribe_candles_request=SubscribeCandlesRequest(
            waiting_close=True,
            subscription_action=SubscriptionAction.SUBSCRIPTION_ACTION_SUBSCRIBE,
            instruments=instruments,
        )
    )
    while True:
        await asyncio.sleep(1)


async def get_metadata(client, uid_list: list, out_queue: asyncio.Queue):
    instruments = [
        CandleInstrument(
            instrument_id=uid,
            interval=SubscriptionInterval.SUBSCRIPTION_INTERVAL_ONE_MINUTE,
        )
        for uid in uid_list
    ]
    while True:
        try:
            print("Connecting to market_data_stream")
            async for metadata in client.market_data_stream.market_data_stream(
                request_iterator(instruments)
            ):
                await out_queue.put(metadata)
        except AioRequestError as e:
            print(f"Stream canceled by server: {e.details}")
        except Exception as e:
            print(f"Unexpected stream error: {repr(e)}")

        print("Reconnecting after 3 seconds")
        await asyncio.sleep(3)


async def extract_candle(shares: dict, in_queue: asyncio.Queue, out_queue: asyncio.Queue):
    while True:
        metadata = await in_queue.get()

        if not metadata.candle:
            continue

        try:
            candle = {
                "ticker": metadata.candle.ticker,
                "uid": metadata.candle.instrument_uid,
                "name": shares[metadata.candle.instrument_uid]["name"],
                "open": quotation_to_float(metadata.candle.open),
                "high": quotation_to_float(metadata.candle.high),
                "low": quotation_to_float(metadata.candle.low),
                "close": quotation_to_float(metadata.candle.close),
                "volume": metadata.candle.volume,
                "timestamp_utc": metadata.candle.time,
            }
            await out_queue.put(candle)
        except AttributeError as e:
            print(f"AttributeError: {str(e)}")
            print({metadata})


async def update_dataframe(in_queue: asyncio.Queue, df: pl.DataFrame, rabbit: RabbitMQPublisher):
    engine = AlertEngine()

    while True:
        buffer = []

        first_candle = await in_queue.get()
        buffer.append(first_candle)
        in_queue.task_done()

        while True:
            try:
                next_candle = await asyncio.wait_for(in_queue.get(), timeout=2.0)
                buffer.append(next_candle)
                in_queue.task_done()
            except asyncio.TimeoutError:
                break

        if buffer:
            new_df = pl.DataFrame(buffer, schema=SCHEMA)
            df = pl.concat([df, new_df])

            # print(f"Minute done\n"
            #       f"Candles added: {len(buffer)}\n"
            #       f"DataFrame length: {df.shape[0]}\n")

            message_list = engine.analyze(df)

            if message_list:
                await rabbit.publish_batch(message_list)

            del buffer
            del new_df


async def main():
    rabbit = RabbitMQPublisher()
    await rabbit.connect()

    async with AsyncClient(INVEST_TOKEN) as client:
        shares = await get_rub_shares(client)
        uid_list = list(shares.keys())

        df = await minute_candles_to_dataframe(client, shares)

        queue_metadata = asyncio.Queue()
        queue_candle = asyncio.Queue()

        try:
            await asyncio.gather(
                get_metadata(client, uid_list, queue_metadata),
                extract_candle(shares, queue_metadata, queue_candle),
                update_dataframe(queue_candle, df, rabbit)
            )
        except asyncio.CancelledError:
            print("Get interrupt")
            raise


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("User interrupt. Program terminated.")
