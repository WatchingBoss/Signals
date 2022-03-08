import concurrent.futures
import os, json
from concurrent.futures import ThreadPoolExecutor
import asyncio
import aiohttp
from datetime import timedelta, datetime, timezone
from time import time

import tinvest as ti
import pandas as pd
import cloudscraper

from ta import myhelper as mh
from ta.stock import Stock
from ta.variables import DELTAS, SUM_COLUMNS, OVERVIEW_COLUMNS
from config import Interval
import ta.scraper as scr


# TODO: Streaming for 1min-day timeframes
# TODO: Update week and month by candle_market each week
# TODO: Signal for previous candle in every timeframe


def update_raw(df: pd.DataFrame, last_row: int, payload: ti.CandleStreaming) -> None:
    df.loc[last_row, 'Time'] = payload.time
    df.loc[last_row, 'Open'] = payload.o
    df.loc[last_row, 'High'] = payload.h
    df.loc[last_row, 'Low'] = payload.l
    df.loc[last_row, 'Close'] = payload.c
    df.loc[last_row, 'Volume'] = payload.v


async def handle_candle(payload: ti.CandleStreaming, stock: Stock):
    tf = stock.timeframes[payload.interval]
    last_row = tf.df.index[-1]
    if (payload.time - tf.df['Time'].iat[last_row]) >= DELTAS[tf.interval]:
        pd.concat([tf.df, pd.DataFrame(pd.Series(dtype=int))], axis=0)
        last_row += 1
    update_raw(tf.df, last_row, payload)


class Scanner:
    def __init__(self, intervals):
        start = time()
        self.intervals = intervals
        self.client = mh.get_client()
        self.usd_stocks = mh.get_market_data(self.client, 'USD', developing=True)
        self.ticker_figi = {s.ticker: s.figi for s in self.usd_stocks.values()}
        print(f"Before filling gets {(time() - start):.2f} sec")

        self.fill_dfs()
        start = time()
        self.fill_indicators()
        print(f"Indicators finished in {(time() - start):.2f} sec")
        self.summeries = [self.sum_df(interval) for interval in intervals]

    def fill_dfs(self) -> None:
        start_read = time()
        with ThreadPoolExecutor(max_workers=8) as ex:
            [ex.submit(s.read_candles, self.intervals) for s in self.usd_stocks.values()]
        print(f"Reading done in {(time() - start_read):.2f} sec")
        for s in self.usd_stocks.values():
            start = time()
            for interval in self.intervals:
                s.fill_df(self.client, interval)
            print(f"Fill_df done for {s.ticker} in {(time() - start):.2f} sec")

    def save_candles(self):
        start_save = time()
        with ThreadPoolExecutor(max_workers=8) as ex:
            [ex.submit(s.save_candles, self.intervals) for s in self.usd_stocks.values()]
        print(f"Saving done in {(time() - start_save):.2f} sec")

    def fill_indicators(self) -> None:
        with ThreadPoolExecutor(max_workers=6) as executor:
            for interval in self.intervals:
                [executor.submit(s.fill_indicators, interval) for s in self.usd_stocks.values()]

    def sum_df(self, interval: ti.CandleResolution) -> pd.DataFrame:
        summery = []
        for s in self.usd_stocks.values():
            last_values = []
            for column in SUM_COLUMNS[1:]:
                try:
                    last_values.append(s.timeframes[interval].df[column].iat[-1])
                except KeyError:
                    last_values.append(None)
            summery.append([s.ticker] + last_values)

        print(f"{interval} Done")
        return pd.DataFrame(summery, columns=SUM_COLUMNS).sort_values(
            by='Ticker', ascending=True, ignore_index=True
        )

    def fill_overview(self):
        scraper = cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'windows',
                'mobile': False
            }
        )
        urls = scr.finviz_urls(list(self.usd_stocks.values()))
        htmls = list()

        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = {executor.submit(scr.downlaod_page_cloudflare, url, scraper) for url in urls}
            for future in concurrent.futures.as_completed(futures):
                htmls.append(future.result())

            futures = {executor.submit(scr.finviz, html) for html in htmls}
            for future in concurrent.futures.as_completed(futures):
                d = future.result()
                self.usd_stocks[self.ticker_figi[d['ticker']]].overview = d

    async def save_overview(self, path: str):
        while True:
            if timedelta(days=1).total_seconds() > (time() - os.path.getatime(path)):
                await asyncio.sleep(timedelta(hours=6).total_seconds())
                continue
            overviews = list()
            for s in self.usd_stocks.values():
                overviews.append(list(s.overview.values()))
            df = pd.DataFrame(overviews, columns=OVERVIEW_COLUMNS).sort_values(
                by='Ticker', ascending=True, ignore_index=True
            )
            df.to_hdf(path, key='df')

    async def streaming(self) -> None:
        async with ti.Streaming(mh.get_token()) as streaming:
            for interval in self.intervals:
                await asyncio.gather(*(streaming.candle.subscribe(s.figi, interval)
                                       for s in self.usd_stocks.values()))
            async for event in streaming:
                await handle_candle(event.payload, self.usd_stocks[event.payload.figi])

    def print_df(self) -> None:
        for df in self.summeries:
            print(df.to_string())

    def save_df(self, paths: list) -> None:
        for i in range(len(self.intervals)):
            df = self.summeries[i]
            df['Ticker'] = df['Ticker'].astype(str)
            df['Time'] = pd.to_datetime(df['Time'], errors='raise', utc=True)
            df[SUM_COLUMNS[2:]] = df[SUM_COLUMNS[2:]].apply(pd.to_numeric, errors='raise')
            self.summeries[i].to_hdf(paths[i], key='df', mode='w')

    async def resave_df(self, paths: list) -> None:
        while True:
            print('Resave function')
            await asyncio.sleep(60)
            self.fill_indicators()
            self.summeries = [self.sum_df(interval) for interval in self.intervals]
            self.save_df(paths)
            self.print_df()


def test_overview():
    client = mh.get_client()
    stocks = list(mh.get_market_data(client, 'USD', developing=True).values())
    urls = scr.finviz_urls(stocks)

    scraper = cloudscraper.create_scraper(
        browser={
            'browser': 'chrome',
            'platform': 'windows',
            'mobile': False
        }
    )

    htmls = []
    finviz_data = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(scr.downlaod_page_cloudflare, url, scraper) for url in urls}
        for future in concurrent.futures.as_completed(futures):
            htmls.append(future.result())

        futures = {executor.submit(scr.finviz, html) for html in htmls}
        for future in concurrent.futures.as_completed(futures):
            finviz_data.append(future.result())

    for d in finviz_data:
        for k, v in d.items():
            print(f"{k}: {v}")
        print()


def test():

    intervals = [
        Interval.min1,
        Interval.min5,
        Interval.min15,
        Interval.min30,
        Interval.hour,
        Interval.day,
        Interval.week,
        Interval.month,
    ]

    # interval = Interval.min1
    client = mh.get_client()
    stocks = mh.get_market_data(client, 'USD', developing=True)
    # p = client.get_market_search_by_ticker('SPCE').payload.instruments[0]
    # s = Stock(ticker=p.ticker, figi=p.figi, isin=p.isin, currency=p.currency)

    with ThreadPoolExecutor(max_workers=8) as ex:
        [ex.submit(s.read_candles, intervals) for s in stocks.values()]

    # with ThreadPoolExecutor(max_workers=8) as ex:
    #     [ex.submit(s.fill_df, client, interval) for s in stocks.values()]
    #
    # with ThreadPoolExecutor(max_workers=8) as ex:
    #     [ex.submit(s.fill_indicators, interval) for s in stocks.values()]

    for s in stocks.values():
        for interval in intervals:
            print(s.timeframes[interval].df.tail(3))

    # summery = []
    # for s in stocks.values():
    #     last_values = []
    #     for column in SUM_COLUMNS[1:]:
    #         try:
    #             last_values.append(s.timeframes[interval].df.iloc[-1][column])
    #         except KeyError:
    #             print(f"Ticker: {s.ticker}\tColumn: {column}\tLenght of Close: {s.timeframes[interval].df['Close'].count()}")
    #             last_values.append(None)
    #     summery.append([s.ticker] + last_values)
    #
    # df = pd.DataFrame(summery, columns=SUM_COLUMNS)
    # print(df.to_string())


def test_tin():
    interval = Interval.min1
    client = mh.get_client()
    stocks = mh.get_market_data(client, 'USD', developing=True)

    from ta.variables import PERIODS
    s = list(stocks.values())[0]
    now = datetime.now(timezone.utc)
    candles = client.get_market_candles(s.figi,
                                        from_=now - PERIODS[interval],
                                        to=now,
                                        interval=ti.CandleResolution(interval)).payload.candles

    candle_list = [[c.time, float(c.o), float(c.h), float(c.l), float(c.c), int(c.v)] for c in candles]

    df = pd.DataFrame(
        candle_list,
        columns=['Time', 'Open', 'High', 'Low', 'Close', 'Volume']
    ).sort_values(by='Time', ascending=True, ignore_index=True)

    print(df.to_string())
