import os
import json
import tinvest as ti
from ta.stock import Stock


def get_token() -> str:
    with open(os.path.join(os.path.expanduser("~"), "no_commit", "info.json")) as f:
        data = json.load(f)
    return data["token_tinkoff_real"]


def get_client() -> ti.SyncClient:
    return ti.SyncClient(get_token())


def get_market_data(client: ti.SyncClient, currency: str, developing: bool = True):
    if developing:
        with open(os.path.join("data", "work_stocks.txt")) as f:
            tickers = f.readline().split(" ")
        stocks = {}
        for t in tickers:
            s = client.get_market_search_by_ticker(t).payload.instruments[0]
            stocks[s.figi] = Stock(s.ticker, s.figi, s.isin, s.currency)
        return stocks
    else:
        payload = client.get_market_stocks().payload
        stocks = [s for s in payload.instruments[:] if s.currency == currency]
        return {s.figi: Stock(s.ticker, s.figi, s.isin, s.currency) for s in stocks}
