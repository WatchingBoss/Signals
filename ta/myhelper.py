import os
import tinvest as ti
from ta.stock import Stock


def get_token() -> str:
    token = os.environ.get('TINKOFF_INVEST_TOKEN')
    if not token:
        # Attempt to fall back to the old method for local development,
        # but print a warning.
        # In a production environment, this fallback should ideally be removed
        # or raise an error immediately.
        fallback_path = os.path.join(os.path.expanduser('~'), 'no_commit', 'info.json')
        try:
            # We need to import json here as it was removed from the top-level imports
            import json
            with open(fallback_path) as f:
                data = json.load(f)
            token = data.get('token_tinkoff_real')
            if token:
                print(f"WARNING: TINKOFF_INVEST_TOKEN not set. Using token from {fallback_path}. "
                      "Please set the TINKOFF_INVEST_TOKEN environment variable for better security and portability.")
            else:
                raise ValueError(f"TINKOFF_INVEST_TOKEN not set and 'token_tinkoff_real' not found in {fallback_path}.")
        except FileNotFoundError:
            raise ValueError(f"TINKOFF_INVEST_TOKEN not set and fallback file {fallback_path} not found.")
        except Exception as e:
            raise ValueError(f"Error loading token from fallback {fallback_path}: {e}")
    if not token: # Double check after potential fallback
        raise ValueError("TINKOFF_INVEST_TOKEN is not set in environment variables.")
    return token


def get_client() -> ti.SyncClient:
    return ti.SyncClient(get_token())


def get_market_data(client: ti.SyncClient, currency: str, developing: bool=True):
    if developing:
        with open(os.path.join('data', 'work_stocks.txt')) as f:
            tickers = f.readline().split(' ')
        stocks = {}
        for t in tickers:
            s = client.get_market_search_by_ticker(t).payload.instruments[0]
            stocks[s.figi] = Stock(s.ticker, s.figi, s.isin, s.currency)
        return stocks
    else:
        payload = client.get_market_stocks().payload
        stocks = [s for s in payload.instruments[:] if s.currency == currency]
        return {s.figi: Stock(s.ticker, s.figi, s.isin, s.currency) for s in stocks}
