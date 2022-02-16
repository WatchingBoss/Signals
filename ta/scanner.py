import os, json
from datetime import datetime, timedelta
import itertools
from concurrent.futures import ThreadPoolExecutor
import asyncio

import tinvest as ti
import pandas as pd

from ta import myhelper as mh
from ta.stock import Stock
from ta.schemas import Interval, SUM_COLUMNS
from ta.variables import DELTAS


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
        for interval in intervals:
            [ex.submit(s.read_candles, interval) for s in stocks.values()]
    for s in stocks.values():
        for interval in intervals:
            s.fill_df(client, interval)
            s.save_candles(interval)
    with ThreadPoolExecutor(max_workers=8) as ex:
        for interval in intervals:
            [ex.submit(s.fill_indicators, interval) for s in stocks.values()]

    for interval in intervals:
        summery = []
        for s in stocks.values():
            last_values = []
            for column in SUM_COLUMNS[1:]:
                try:
                    last_values.append(s.timeframes[interval].df.iloc[-1][column])
                except KeyError:
                    print(f"Ticker: {s.ticker}\tColumn: {column}\tLenght of Close: {s.timeframes[interval].df['Close'].count()}")
                    last_values.append(None)
            summery.append([s.ticker] + last_values)

        df = pd.DataFrame(summery, columns=SUM_COLUMNS)
        print(df.to_string())


class Scanner:
    def __init__(self, intervals):
        self.intervals = intervals
        self.client = mh.get_client()
        self.usd_stocks = mh.get_market_data(self.client, 'USD', developing=True)

        self.fill_dfs()
        self.fill_indicators()
        self.summeries = [self.sum_df(interval) for interval in intervals]

    def fill_dfs(self) -> None:
        with ThreadPoolExecutor(max_workers=8) as ex:
            for interval in self.intervals:
                [ex.submit(s.read_candles, interval) for s in self.usd_stocks.values()]
        for s in self.usd_stocks.values():
            for interval in self.intervals:
                s.fill_df(self.client, interval)
            print(f"Fill_df done for {s.ticker}")
        with ThreadPoolExecutor(max_workers=8) as ex:
            for interval in self.intervals:
                [ex.submit(s.save_candles, interval) for s in self.usd_stocks.values()]

    def fill_indicators(self) -> None:
        with ThreadPoolExecutor(max_workers=6) as executor:
            for interval in self.intervals:
                [executor.submit(s.fill_indicators, interval) for s in self.usd_stocks.values()]

    async def update_indicators(self) -> None:
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


def overview():
    with open(os.path.join(os.path.expanduser('~'), 'no_commit', 'info.json')) as f:
        data = json.load(f)
    client = ti.SyncClient(data['token_tinkoff_real'])

    path_data_dir = os.path.join(os.curdir, 'data')

    stocks = {}

    columns = ['Name', 'Price', 'Sector', 'Industry', 'P/E', 'P/S',
               'Debt/Equity', 'ATR', 'Average Volume', 'Short Float']
    index = []
    rows = []
    for stock in stocks.values():
        index.append(stock.ticker)
        rows.append([stock.data['name'], stock.data['price'], stock.data['sector'],
                     stock.data['industry'], stock.data['p_e'], stock.data['p_s'], stock.data['debt_eq'],
                     stock.data['atr'], stock.data['avg_v'], stock.data['short_float']])

    df = pd.DataFrame(rows, index=index, columns=columns)
    df.sort_index(inplace=True)

    pkl_file = os.path.join(path_data_dir, 'overview.pkl')
    df.to_pickle(pkl_file)
