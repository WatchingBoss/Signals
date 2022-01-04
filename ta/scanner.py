import os, json

import tinvest
import pandas as pd
import myhelper as mh

from ta.stock import Stock


class Scanner:
    def __init__(self):
        self.client = mh.get_client()
        self.usd_stocks = mh.get_market_data(self.client, 'USD')


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

