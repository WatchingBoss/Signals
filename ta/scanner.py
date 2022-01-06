import os, json
from concurrent.futures import ThreadPoolExecutor

import tinvest
import pandas as pd

from ta.stock import Stock
from ta import myhelper as mh


class Scanner:
    def __init__(self):
        self.client = mh.get_client()
        self.usd_stocks = mh.get_market_data(self.client, 'USD', developing=True)
        self.df = pd.DataFrame(columns=['Ticker', 'Time', 'Open', 'High', 'Low', 'Close', 'Volume',
                                        'SMA10', 'SMA20', 'SMA50', 'SMA100', 'SMA200',
                                        'EMA10', 'EMA20', 'EMA50', 'EMA100', 'EMA200',
                                        'MACD_hist', 'RSI', 'ATR10'])
        self.fill_all_stocks()

    def fill_all_stocks(self):
        with ThreadPoolExecutor(max_workers=5) as executor:
            [executor.submit(s.fill_df, self.client, s.m1) for s in self.usd_stocks.values()]
            [executor.submit(s.fill_indicators, s.m1) for s in self.usd_stocks.values()]

    def update_indicators(self):
        for s in self.usd_stocks.values():
            s.fill_indicators(s.m1)

    def sum_df(self):
        last_values = [[s.ticker] + s.m1.df.tail(1).values.tolist()[0]
                       for s in self.usd_stocks.values()]
        self.df = pd.DataFrame(last_values, columns=self.df.columns)\
                  .sort_values(by='Ticker', ascending=True, ignore_index=True)


    def print_dfs(self):
        print(self.df.to_string())

    def save_df(self, path):
        self.df.to_pickle(path)



def overview():
    with open(os.path.join(os.path.expanduser('~'), 'no_commit', 'info.json')) as f:
        data = json.load(f)
    client = tinvest.SyncClient(data['token_tinkoff_real'])

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

