import os, json, datetime
from concurrent.futures import ThreadPoolExecutor
import asyncio

import tinvest as ti
import pandas as pd

from ta import myhelper as mh
from ta.stock import Stock
from ta.schemas import Interval


TITLE = [
    'Ticker', 'Time', 'Open', 'High', 'Low', 'Close', 'Volume',
    'EMA10', 'EMA20', 'EMA100', 'EMA200',
    'MACD_hist', 'RSI', 'ATR10'
    ]


def update_raw(df: pd.DataFrame, payload: ti.CandleStreaming) -> pd.DataFrame:
    df.loc[-1, 'Time'] = payload.time
    df.loc[-1, 'Open'] = payload.o
    df.loc[-1, 'High'] = payload.h
    df.loc[-1, 'Low'] = payload.l
    df.loc[-1, 'Close'] = payload.c
    df.loc[-1, 'Volume'] = payload.v
    return df


async def handle_candle(payload: ti.CandleStreaming, stock: Stock):
    tf = stock.timeframes[payload.interval]
    if tf.df['Time'].iat[-1] < payload.time:
        print(f'{stock.ticker}: New row')
        tf.df.append(pd.Series(), ignore_index=True)
        tf.df = update_raw(tf.df, payload)
    elif tf.df['Time'].iat[-1] == payload.time:
        print(f'{stock.ticker}: Update this raw')
        tf.df = update_raw(tf.df, payload)


def test():
    client = mh.get_client()
    # stocks = list(mh.get_market_data(client, 'USD').values())
    p = client.get_market_search_by_ticker('WFC').payload.instruments[0]
    s = Stock(ticker=p.ticker, figi=p.figi, isin=p.isin, currency=p.currency)
    interval = Interval.min30
    s.fill_df(client, interval)
    print(s.timeframes[interval].df)
    # for s in stocks:
    #     s.fill_df(client, interval)
    # with ThreadPoolExecutor(max_workers=6) as ex:
    #     [ex.submit(s.fill_indicators, interval) for s in stocks]
    #
    # last_values = [[s.ticker] + s.timeframes[interval].df.tail(1).values.tolist()[-1]
    #                for s in stocks]
    # df = pd.DataFrame(last_values, columns=TITLE)
    # print(df.to_string())


class Scanner:
    def __init__(self, intervals):
        self.intervals = intervals
        self.client = mh.get_client()
        self.usd_stocks = mh.get_market_data(self.client, 'USD', developing=True)

        self.fill_dfs()
        self.fill_indicators()
        self.summeries = [self.sum_df(interval) for interval in intervals]

    def fill_dfs(self) -> None:
        for s in self.usd_stocks.values():
            for interval in self.intervals:
                s.fill_df(self.client, interval)
            print(f"Fill_df done for {s.ticker}")

    def fill_indicators(self) -> None:
        with ThreadPoolExecutor(max_workers=6) as executor:
            for interval in self.intervals:
                [executor.submit(s.fill_indicators, interval) for s in self.usd_stocks.values()]

    async def update_indicators(self) -> None:
        with ThreadPoolExecutor(max_workers=6) as executor:
            for interval in self.intervals:
                [executor.submit(s.fill_indicators, interval) for s in self.usd_stocks.values()]

    def sum_df(self, interval: ti.CandleResolution) -> pd.DataFrame:
        last_values = [[s.ticker] + s.timeframes[interval].df.tail(1).values.tolist()[-1]
                       for s in self.usd_stocks.values()]
        print(f"{interval} Done")
        return pd.DataFrame(last_values, columns=TITLE).sort_values(
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
            self.summeries[i].to_pickle(paths[i])

    async def resave_df(self, paths: list) -> None:
        while True:
            print('Resave function')
            await asyncio.sleep(30)
            await self.update_indicators()
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

