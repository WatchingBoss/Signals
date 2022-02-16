import os
from schemas import Interval

basedir = os.path.abspath(os.path.dirname(__file__))

class Paths(object):
    data_dir = os.path.join(basedir, 'data')
    candles_dir = os.path.join(data_dir, 'candles')
    sum_dir = os.path.join(data_dir, 'summeries')


class Config(object):
    SECRET_KEY = os.environ.get('SECRET_KEY')
    SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URI') or f"sqlite:///{os.path.join(basedir, 'data', 'app.db')}"
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    DATA_DIR = Paths.data_dir
    CANDLES_DIR = Paths.candles_dir
    SUM_DIR = Paths.sum_dir


intervals = [
    Interval.min1,
    Interval.min5,
    Interval.min15,
    Interval.min30,
    Interval.hour,
    Interval.day,
    Interval.week,
    Interval.month,
]


SUM_COLUMNS = [
    'Ticker', 'Time',
    'Open', 'High', 'Low', 'Close', 'Volume',
    'EMA_10', 'EMA_20', 'EMA_50', 'EMA_200',
    'RSI_14',
    'MACDh_12_26_9'
]

CANDLE_COLUMNS = [
    'Time',
    'Open', 'High', 'Low', 'Close', 'Volume',
    'EMA_10', 'EMA_20', 'EMA_50', 'EMA_200',
    'RSI_14',
    'MACD_12_26_9', 'MACDh_12_26_9', 'MACDs_12_26_9'
]
