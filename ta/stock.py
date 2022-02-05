from datetime import datetime, timedelta, timezone
import time
from ta import scraper
from ta.schemas import Interval
import tinvest as ti
import pandas as pd
import pandas_ta as ta


FirstStrategy = ta.Strategy(
    name='First Strategy',
    ta=[
        {'kind': 'ema', 'length': 10},
        {'kind': 'ema', 'length': 20},
        {'kind': 'ema', 'length': 50},
        {'kind': 'ema', 'length': 200},
        {'kind': 'rsi'},
        {'kind': 'macd', 'fast': 12, 'slow': 26, 'signal': 9},
    ]
)


YahooIntervals = {
    Interval.min1: '1m',
    Interval.min5: '5m',
    Interval.min15: '15m',
    Interval.min30: '30m',
    Interval.hour: '1h',
    Interval.day: '1d',
    Interval.week: '1wk',
    Interval.month: '1mo'
}


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

    def check_if_able_for_short(self):
        self.shortable = scraper.check_tinkoff_short_table(self.isin)

    def get_intervals(self) -> tuple:
        return tuple(self.timeframes.keys())

    def fill_df(self, client, interval: Interval):
        tf = self.timeframes[interval]

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

        tf.df = pd.DataFrame(
            candle_list,
            columns=['Time', 'Open', 'High', 'Low', 'Close', 'Volume']
        ).sort_values(by='Time', ascending=True, ignore_index=True)

    def fill_indicators(self, interval: Interval):
        tf = self.timeframes[interval]
        tf.df.ta.cores = 0
        tf.df.ta.strategy(FirstStrategy)

        # temp = tf.df.copy()
        # temp['Time'] = temp['Time'].apply(lambda x: x.replace(tzinfo=None))
        # temp = temp.set_index(pd.DatetimeIndex(tf.df['Time']))
        # tf.df['VWAP'] = temp.ta.vwap().values

        cdl_df = tf.df.ta.cdl_pattern(name=['hammer', 'invertedhammer', 'engulfing'])
        tf.df = pd.concat([tf.df, cdl_df], axis=1)

        tf.df.rename(columns={
            'MACD_12_26_9': 'MACD',
            'MACDh_12_26_9': 'MACD_Hist',
            'MACDs_12_26_9': 'MACD_Signal',
        }, inplace=True)

    def fill_df_yahoo(self, interval):
        tf = self.timeframes[interval]
        tf.df = tf.df.ta.ticker(self.ticker, period='1mo', interval=YahooIntervals[interval])