from datetime import datetime, timedelta, timezone
import time
from ta import scraper
from ta.indicators import sma, ema, macd, rsi, atr
import tinvest as ti
import pandas as pd


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

    def __init__(self, interval: ti.CandleResolution):
        self.df = pd.DataFrame()

        self.interval = interval
        self.delta = timedelta(hours=6, minutes=30)

        self.last_modify_time = datetime.now(tz=timezone.utc)

    def sma(self, column_name, period):
        self.df = sma(self.df, 'Close', column_name, period)

    def ema(self, column_name, period):
        self.df = ema(self.df, 'Close', column_name, period, False)

    def macd(self):
        self.df = macd(self.df, 12, 26, 9, 'Close')

    def rsi(self):
        self.df = rsi(self.df, 'Close', 14)

    def atr(self, period):
        self.df = atr(self.df, period, ['Open', 'High', 'Low', 'Close'])


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
        self.shortable = False

        self.m1 = Timeframe(ti.CandleResolution.min1)
        self.m5 = Timeframe(ti.CandleResolution.min5)
        self.m15 = Timeframe(ti.CandleResolution.min15)

    def __lt__(self, another):
        return self.ticker < another.ticker

    async def __aiter__(self):
        return self

    def check_if_able_for_short(self):
        self.shortable = scraper.check_tinkoff_short_table(self.isin)

    def return_tfs(self):
        return [self.m1, self.m5, self.m15]

    def fill_df(self, client, tf: Timeframe):
        start = datetime.utcnow()
        list_size = 250
        candle_list = []
        for _ in range(10):
            if len(candle_list) >= list_size:
                break
            try:
                candles = client.get_market_candles(self.figi,
                                                    from_=start - timedelta(days=1),
                                                    to=start,
                                                    interval=tf.interval).payload.candles
                start -= timedelta(days=1)
                for c in candles:
                    candle_list.append([c.time + timedelta(hours=3), c.o, c.h, c.l, c.c, c.v])
            except ti.exceptions.TooManyRequestsError:
                time.sleep(60)
        tf.df = pd.DataFrame(candle_list, columns=['Time', 'Open', 'High', 'Low', 'Close', 'Volume'])\
                .sort_values(by='Time', ascending=True, ignore_index=True)
        tf.df['Time'] = tf.df['Time'].apply(lambda t: t.strftime("%y-%m-%d %H:%M"))

    def fill_indicators(self, tf: Timeframe):
        for period in [10, 20, 50, 200]:
            tf.sma(f'SMA{period}', period)
        for period in [10, 20, 50, 200]:
            tf.ema(f'EMA{period}', period)
        tf.macd()
        tf.rsi()
        tf.atr(10)
