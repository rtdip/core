from enum import IntFlag, auto


class ValueType(IntFlag):
    # Keep the order when refactoring.
    counter = auto()
    gauge = auto()
    histogram = auto()
    summary = auto()
    usage = auto()
    generation = auto()
    prediction = auto()
    short_term = auto()
    long_term = auto()
    backcast = auto()
    forecast = auto()
    short_term_backcast = short_term | backcast
    long_term_term_backcast = long_term | backcast
    short_term_forecast = short_term | forecast
    long_term_term_forecast = long_term | forecast
    # ...






