from datetime import datetime, timedelta, timezone
import time
from ta import scraper
from ta.indicators import sma, ema, macd, rsi, atr
from ta.schemas import Interval
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

    def __init__(self, interval: Interval):
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


class Stock(Instrument):
    """
    Stock implementation
    """
    def __init__(self, ticker: str, figi: str, isin: str, currency: str):
        super().__init__(ticker, figi, isin, currency)
        self.shortable = False

        self.timeframes = {
            Interval.min1: Timeframe(Interval.min1),
            Interval.min5: Timeframe(Interval.min5),
            Interval.min15: Timeframe(Interval.min15),
            Interval.min30: Timeframe(Interval.min30),
            Interval.hour: Timeframe(Interval.hour),
            Interval.day: Timeframe(Interval.day),
            Interval.week: Timeframe(Interval.week),
            Interval.month: Timeframe(Interval.month),
        }

    def __lt__(self, another):
        return self.ticker < another.ticker

    async def __aiter__(self):
        return self

    def check_if_able_for_short(self):
        self.shortable = scraper.check_tinkoff_short_table(self.isin)

    def get_intervals(self) -> tuple:
        return tuple(self.timeframes.keys())

    def fill_df(self, client, interval: Interval):
        delta = timedelta(days=1)
        if interval is Interval.hour:
            delta = timedelta(days=7)
        elif interval is Interval.day:
            delta = timedelta(days=365)
        elif interval is Interval.week:
            delta = timedelta(days=365*1.8)
        elif interval is Interval.month:
            delta = timedelta(days=365*10)

        start = datetime.utcnow()
        list_size = 250
        candle_list = []
        last_date = datetime.utcnow().timestamp()
        min_date = (datetime.utcnow() - timedelta(minutes=10)).timestamp()
        break_loop = 0

        while break_loop < 4:
            if len(candle_list) >= list_size:
                break
            if min_date < last_date:
                last_date = min_date
            else:
                break_loop += 1

            try:
                candles = client.get_market_candles(self.figi,
                                                    from_=start - delta,
                                                    to=start,
                                                    interval=interval).payload.candles
                if len(candles) > 1:
                    min_date = candles[0].time.timestamp()
                else:
                    break_loop += 1
                start -= delta

                candle_list += [[c.time, float(c.o), float(c.h), float(c.l), float(c.c), int(c.v)]
                                for c in candles]

            except ti.exceptions.TooManyRequestsError:
                print(f"Wating for 60 seconds -> {self.ticker} -> {interval} -> {datetime.now().strftime('%H:%M:%S')}")
                time.sleep(60)
        self.timeframes[interval].df = pd.DataFrame(
            candle_list,
            columns=['Time', 'Open', 'High', 'Low', 'Close', 'Volume']
        ).sort_values(by='Time', ascending=True, ignore_index=True)

    def fill_indicators(self, interval: Interval):
        tf = self.timeframes[interval]
        for period in [10, 20, 50, 200]:
            tf.ema(f'EMA{period}', period)
        tf.macd()
        tf.rsi()
        tf.atr(10)
