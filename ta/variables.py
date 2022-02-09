from datetime import timedelta, datetime
from ta.schemas import Interval


DELTAS = {
    Interval.min1: timedelta(minutes=1),
    Interval.min5: timedelta(minutes=5),
    Interval.min15: timedelta(minutes=15),
    Interval.min30: timedelta(minutes=30),
    Interval.hour: timedelta(hours=1),
    Interval.day: timedelta(days=1),
    Interval.week: timedelta(days=7),
    Interval.month: timedelta(days=30)
}

PERIODS = {
    Interval.min1: timedelta(days=1),
    Interval.min5: timedelta(days=1),
    Interval.min15: timedelta(days=1),
    Interval.min30: timedelta(days=1),
    Interval.hour: timedelta(days=7),
    Interval.day: timedelta(days=365),
    Interval.week: timedelta(days=365*1.8),
    Interval.month: timedelta(days=365*10)
}