import os, json
import tinvest as ti
from ta.stock import Stock


def get_market_data(client, currency, developing=True):
    if developing:
        with open(os.path.join('data', 'work_stocks.txt')) as f:
            tickers = f.readline().split(' ')
    else:
        payload = client.get_market_stocks().payload
        stocks = [s for s in payload.instruments[:] if s.currency == currency]
        return {s.figi: Stock(s.ticker, s.figi, s.isin, s.currency) for s in stocks}


def get_client():
    with open(os.path.join(os.path.expanduser('~'), 'no_commit', 'info.json')) as f:
        data = json.load(f)
    return ti.SyncClient(data['token_tinkoff_real'])
