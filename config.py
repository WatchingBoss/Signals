import os
from ta.schemas import Interval

basedir = os.path.abspath(os.path.dirname(__file__))


class Config(object):
    SECRET_KEY = os.environ.get('SECRET_KEY')
    SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URI') or f"sqlite:///{os.path.join(basedir, 'data', 'app.db')}"
    SQLALCHEMY_TRACK_MODIFICATIONS = False


intervals = [
    Interval.min1,
    # Interval.min5,
    # Interval.min15,
    # Interval.min30,
    # Interval.hour,
    # Interval.day,
    # Interval.week,
    # Interval.month,
]
