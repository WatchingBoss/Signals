from datetime import datetime, timedelta, timezone
import asyncio
import scraper
import tinvest
import pandas as pd
import numpy as np


SIGNAL = []


class Instrument:
    """
    Exchange instrument imlementation
    """
    def __init__(self, ticker: str, figi: str, isin: str, currency: str):
        self.ticker = ticker
        self.figi = figi
        self.isin = isin
        self.currency = currency


class Timeframe:
    """
    Timeframe implementation
    """

    def __init__(self, interval: tinvest.CandleResolution):
        self.candles = pd.DataFrame()

        self.indicators = pd.DataFrame()

        self.interval = interval
        self.delta = timedelta(hours=6, minutes=30)

        self.last_modify_time = datetime.now(tz=timezone.utc)

    def ema(self, column_name, period):
        self.candles = ema(self.candles, 'Close', column_name, period, False)

    def macd(self):
        self.candles = macd(self.candles, 12, 26, 9, 'Close')

    def rsi(self):
        self.candles = rsi(self.candles, 'Close', 14)


class Event:
    def __init__(self, tf: Timeframe, msg):
        self.tf = tf
        self.msg = msg


class Stock(Instrument):
    """
    Stock implementation
    """
    def __init__(self, ticker: str, figi: str, isin: str, currency: str):
        super().__init__(ticker, figi, isin, currency)
        self.data = {}
        self.able_for_short = False

        self.m1 = Timeframe(tinvest.CandleResolution.min1)
        self.m5 = Timeframe(tinvest.CandleResolution.min5)
        self.m15 = Timeframe(tinvest.CandleResolution.min15)

    def __lt__(self, another):
        return self.ticker < another.ticker

    async def __aiter__(self):
        return self

    def add_scraping_data(self):
        if self.currency == 'USD':
            self.data = scraper.check_finviz(self.ticker)

    def check_if_able_for_short(self):
        self.able_for_short = scraper.check_tinkoff_short_table(self.isin)

    def output_data(self):
        return_data = {
            'ticker': self.ticker,
            'figi': self.figi,
            'isin': self.isin,
            'currency': self.currency,
            'data': self.data
        }
        return return_data


def ema(df, base, target, period, alpha=False):
    con = pd.concat([df[:period][base].rolling(window=period).mean(), df[period:][base]])

    if alpha:
        # (1 - alpha) * previous_val + alpha * current_val where alpha = 1 / period
        df[target] = con.ewm(alpha=1 / period, adjust=False).mean()
    else:
        # ((current_val - previous_val) * coeff) + previous_val where coeff = 2 / (period + 1)
        df[target] = con.ewm(span=period, adjust=False).mean()

    df[target].fillna(0, inplace=True)
    return df


def macd(df, fast_ema=12, slow_ema=26, signal=9, base='Close'):
    fast_col = "ema_" + str(fast_ema)
    slow_col = "ema_" + str(slow_ema)
    macd_col = "macd"
    sig = "signal"
    hist = "hist"

    # Compute fast and slow EMA
    ema(df, base, fast_col, fast_ema)
    ema(df, base, slow_col, slow_ema)

    # Compute MACD
    df[macd_col] = np.where(np.logical_and(np.logical_not(df[fast_col] == 0), np.logical_not(df[slow_col] == 0)),
                            df[fast_col] - df[slow_col], 0)

    # Compute MACD Signal
    ema(df, macd_col, sig, signal)

    # Compute MACD Histogram
    df[hist] = np.where(np.logical_and(np.logical_not(df[macd_col] == 0), np.logical_not(df[sig] == 0)),
                        df[macd_col] - df[sig], 0)

    return df


def rsi(df, base="Close", period=14):
    delta = df[base].diff()
    up, down = delta.copy(), delta.copy()

    up[up < 0] = 0
    down[down > 0] = 0

    r_up = up.ewm(com=period - 1, adjust=False).mean()
    r_down = down.ewm(com=period - 1, adjust=False).mean().abs()

    df['rsi'] = 100 - 100 / (1 + r_up / r_down)
    df['rsi'].fillna(0, inplace=True)

    return df
