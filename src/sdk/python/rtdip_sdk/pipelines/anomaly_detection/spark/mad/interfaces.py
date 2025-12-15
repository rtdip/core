import pandas as pd
from abc import ABC, abstractmethod


class MadScorer(ABC):
    def __init__(self, threshold: float = 3.5):
        self.threshold = threshold

    @abstractmethod
    def score(self, series: pd.Series) -> pd.Series:
        pass

    def is_anomaly(self, scores: pd.Series) -> pd.Series:
        return scores.abs() > self.threshold
