import os
from enum import Enum
import tinvest as ti


basedir = os.path.abspath(os.path.dirname(__file__))

class Paths(object):
    data_dir = os.path.join(basedir, 'data')
    candles_dir = os.path.join(data_dir, 'candles')
    sum_dir = os.path.join(data_dir, 'summeries')
    overview = os.path.join(data_dir, 'overview')


class Config(object):
    SECRET_KEY = os.environ.get('SECRET_KEY')
    SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URI') or f"sqlite:///{os.path.join(basedir, 'data', 'app.db')}"
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    DATA_DIR = Paths.data_dir
    CANDLES_DIR = Paths.candles_dir
    SUM_DIR = Paths.sum_dir


class Interval(str, Enum):
    min1 = ti.CandleResolution.min1.value
    min5 = ti.CandleResolution.min5.value
    min15 = ti.CandleResolution.min15.value
    min30 = ti.CandleResolution.min30.value
    hour = ti.CandleResolution.hour.value
    day = ti.CandleResolution.day.value
    week = ti.CandleResolution.week.value
    month = ti.CandleResolution.month.value


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


