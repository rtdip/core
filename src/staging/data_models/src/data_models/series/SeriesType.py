from enum import IntFlag, auto


class SeriesType(IntFlag):
    # Keep the order when refactoring.
    real_time = auto()
    minute_1 = auto()
    minutes_5 = auto()
    minutes_10 = auto()
    minutes_15 = auto()
    minutes_30 = auto()
    minutes_60 = auto()
    minutes_2_hours = auto()
    minutes_3_hours = auto()
    minutes_4_hours = auto()
    minutes_6_hours = auto()
    minutes_8_hours = auto()
    minutes_12_hours = auto()
    minutes_24_hours = auto()
    hour = auto()
    day = auto()
    week = auto()
    month = auto()
    year = auto()
    # Computations
    sum = auto()
    average_filter = auto()
    max_filter = auto()
    min_filter = auto()
    # Testing
    test = auto()



