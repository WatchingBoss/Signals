import os, sys, json, time
from datetime import datetime, timedelta, timezone
import asyncio
import concurrent.futures

import tinvest
import numpy as np
import pandas as pd
from tradingview_ta import TA_Handler, Interval, Exchange

from stock import Stock, Timeframe


def get_market_stocks(client, stocks_dict):
    payload = client.get_market_stocks().payload
    stocks_usd = [stock for stock in payload.instruments[:] if stock.currency == 'USD']

    for i in range(len(stocks_usd)):
        stock = stocks_usd[i]
        stocks_dict[stock.figi] = Stock(stock.ticker, stock.figi, stock.isin, stock.currency)


def load_stocks(file, client, stocks_dict):
    with open(file, 'r') as f:
        tickers = f.readline().split(' ')
    for t in tickers:
        stock = client.get_market_search_by_ticker(t).payload.instruments[0]
        stocks_dict[stock.figi] = Stock(stock.ticker, stock.figi, stock.isin, stock.currency)


def scrinner():
    with open(os.path.join(os.path.expanduser('~'), 'no_commit', 'info.json')) as f:
        data = json.load(f)
    client = tinvest.SyncClient(data['token_tinkoff_real'])

    path_data_dir = os.path.join(os.curdir, 'data')
    path_stocks_usd = os.path.join(path_data_dir, 'stocks_usd' + '.json')
    if not os.path.isdir(path_data_dir):
        os.mkdir(path_data_dir)

    stocks = {}
    load_stocks(os.path.join('data', 'work_stocks.txt'), client, stocks)

    for k, v in stocks.items():
        v.add_scraping_data()

    columns = ['Name', 'Price', 'Sector', 'Industry', 'P/E', 'P/S',
               'Debt/Equity', 'ATR', 'Average Volume', 'Short Float']
    rows = []
    tickers = []
    for stock in stocks.values():
        tickers.append(stock.ticker)
        rows.append([stock.data['name'], stock.data['price'], stock.data['sector'], stock.data['industry'],
                     stock.data['p_e'], stock.data['p_s'], stock.data['debt_eq'], stock.data['atr'],
                     stock.data['avg_v'], stock.data['short_float']])

    df = pd.DataFrame(rows, index=tickers, columns=columns)

    return df
