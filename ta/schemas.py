from enum import Enum
import tinvest as ti
import pandas_ta as ta


class Interval(str, Enum):
    min1 = ti.CandleResolution.min1.value
    min5 = ti.CandleResolution.min5.value
    min15 = ti.CandleResolution.min15.value
    min30 = ti.CandleResolution.min30.value
    hour = ti.CandleResolution.hour.value
    day = ti.CandleResolution.day.value
    week = ti.CandleResolution.week.value
    month = ti.CandleResolution.month.value


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


SUM_COLUMNS = [
    'Ticker', 'Time',
    'Open', 'High', 'Low', 'Close', 'Volume',
    'EMA_10', 'EMA_20', 'EMA_50', 'EMA_200',
    'RSI_14',
    'MACDh_12_26_9'
]

