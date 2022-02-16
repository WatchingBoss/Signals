from datetime import datetime, timedelta, timezone
import time, os
from config import Paths, CANDLE_COLUMNS
from ta import scraper
from schemas import Interval
from ta.variables import DELTAS, PERIODS, YahooIntervals
import tinvest as ti
import pandas as pd
import pandas_ta as ta


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
        self.cdl = pd.DataFrame()
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
            Interval.month: Timeframe(Interval.month)
        }

    def __lt__(self, another):
        return self.ticker < another.ticker

    def check_if_able_for_short(self):
        self.shortable = scraper.check_tinkoff_short_table(self.isin)

    def save_candles(self, interval: Interval) -> None:
        path = os.path.join(Paths.candles_dir, self.ticker + '_' + interval.value + '.h5')
        df = self.timeframes[interval].df
        df['Time'] = pd.to_datetime(df['Time'], errors='raise', utc=True)
        df[CANDLE_COLUMNS[1:]] = df[CANDLE_COLUMNS[1:]].apply(pd.to_numeric, errors='raise')
        df.to_hdf(path, key='df', mode='w')

    def read_candles(self, interval: Interval) -> None:
        path = os.path.join(Paths.candles_dir, self.ticker + '_' + interval.value + '.h5')
        if not os.path.isfile(path):
            return
        self.timeframes[interval].df = pd.read_hdf(path)

    def fill_df(self, client, interval: Interval):
        tf = self.timeframes[interval]

        if not tf.df.size:
            tf.df = fill_df(client, interval, self.ticker, self.figi)
        else:
            if (datetime.now(timezone.utc) - tf.df['Time'].iat[-1]) > DELTAS[interval]:
                tf.df = pd.concat([tf.df, append_df(client, interval, tf.df['Time'].iat[-1], self.figi)],
                                 ignore_index=True)
                tf.df = tf.df.drop_duplicates(subset=['Time'])

    def fill_indicators(self, interval: Interval):
        tf = self.timeframes[interval]

        tf.df['EMA_10'] = ta.ema(tf.df['Close'], length=10)
        tf.df['EMA_20'] = ta.ema(tf.df['Close'], length=20)
        tf.df['EMA_50'] = ta.ema(tf.df['Close'], length=50)
        tf.df['EMA_200'] = ta.ema(tf.df['Close'], length=200)
        tf.df['RSI_14'] = ta.rsi(tf.df['Close'])
        tf.df[['MACD_12_26_9', 'MACDh_12_26_9', 'MACDs_12_26_9']] = ta.macd(tf.df['Close'], fast=12, slow=26, signal=9)

        # temp = tf.df.copy()
        # temp['Time'] = temp['Time'].apply(lambda x: x.replace(tzinfo=None))
        # temp = temp.set_index(pd.DatetimeIndex(tf.df['Time']))
        # tf.df['VWAP'] = temp.ta.vwap().values

        tf.cdl = tf.df.ta.cdl_pattern(name=['hammer', 'invertedhammer', 'engulfing'])

    def fill_df_yahoo(self, interval):
        tf = self.timeframes[interval]
        tf.df = tf.df.ta.ticker(self.ticker, period='1mo', interval=YahooIntervals[interval])


def append_df(client: ti.SyncClient, interval: Interval, last_time: datetime, figi: str) -> pd.DataFrame:
    now = datetime.now(timezone.utc)
    oldest_time = datetime.now(timezone.utc)
    candle_list = []
    break_loop = 0
    while oldest_time > last_time and break_loop < 4:
        try:
            candles = client.get_market_candles(figi,
                                                from_=now - PERIODS[interval],
                                                to=now,
                                                interval=ti.CandleResolution(interval)).payload.candles
            if len(candles) > 1:
                oldest_time = candles[0].time
            else:
                break_loop += 1
            now -= PERIODS[interval]

            candle_list += [[c.time, float(c.o), float(c.h), float(c.l), float(c.c), int(c.v)]
                            for c in candles]

        except ti.exceptions.TooManyRequestsError:
            print(f"Wating for 60 seconds -> {figi} -> {interval} -> {datetime.now().strftime('%H:%M:%S')}")
            time.sleep(60)

    df = pd.DataFrame(
        candle_list,
        columns=['Time', 'Open', 'High', 'Low', 'Close', 'Volume']
    ).sort_values(by='Time', ascending=True, ignore_index=True)
    return df.loc[df['Time'] > last_time]


def fill_df(client, interval, ticker, figi) -> pd.DataFrame:
    now = datetime.utcnow()
    list_size = 250
    candle_list = []
    last_date = datetime.now(timezone.utc)
    min_date = datetime.now(timezone.utc) - timedelta(minutes=3)
    break_loop = 0

    while break_loop < 4:
        if len(candle_list) >= list_size:
            break
        if min_date < last_date:
            last_date = min_date
        else:
            break_loop += 1

        try:
            candles = client.get_market_candles(figi,
                                                from_=now - PERIODS[interval],
                                                to=now,
                                                interval=interval).payload.candles
            if len(candles) > 1:
                min_date = candles[0].time
            else:
                break_loop += 1
            now -= PERIODS[interval]

            candle_list += [[c.time, float(c.o), float(c.h), float(c.l), float(c.c), int(c.v)]
                            for c in candles]

        except ti.exceptions.TooManyRequestsError:
            print(f"Wating for 60 seconds -> {ticker} -> {interval} -> {datetime.now().strftime('%H:%M:%S')}")
            time.sleep(60)

    return pd.DataFrame(
        candle_list,
        columns=['Time', 'Open', 'High', 'Low', 'Close', 'Volume']
    ).sort_values(by='Time', ascending=True, ignore_index=True)
