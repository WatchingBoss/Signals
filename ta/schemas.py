from enum import Enum
import tinvest as ti

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
    # Interval.min15,
    # Interval.min30,
    # Interval.hour,
    # Interval.day,
    # Interval.week,
    # Interval.month,
]
