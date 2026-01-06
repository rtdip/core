import pandas as pd
from typing import Optional

from rtdip_sdk.pipelines.interfaces import PipelineComponent
from .interfaces import IQRAnomalyDetectionConfig


class IQRAnomalyDetectionComponent(PipelineComponent):
    """
    RTDIP component implementing IQR-based anomaly detection.

    Supports:
    - Global IQR (window=None)
    - Rolling IQR (window=int)
    """

    def __init__(self, config: IQRAnomalyDetectionConfig):
        self.k: float = config.get("k", 1.5)
        self.window: Optional[int] = config.get("window", None)

        self.value_column: str = config.get("value_column", "value")
        self.time_column: str = config.get("time_column", "timestamp")

    def run(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Run IQR anomaly detection on a time series DataFrame.

        Input:
            df with columns [time_column, value_column]

        Output:
            df with additional column:
            - is_anomaly (bool)
        """

        if self.value_column not in df.columns:
            raise ValueError(
                f"Column '{self.value_column}' not found in input DataFrame"
            )

        values = df[self.value_column]

        # -----------------------
        # Global IQR
        # -----------------------
        if self.window is None:
            q1 = values.quantile(0.25)
            q3 = values.quantile(0.75)
            iqr = q3 - q1

            lower = q1 - self.k * iqr
            upper = q3 + self.k * iqr

        # -----------------------
        # Rolling IQR
        # -----------------------
        else:
            q1 = values.rolling(self.window).quantile(0.25)
            q3 = values.rolling(self.window).quantile(0.75)
            iqr = q3 - q1

            lower = q1 - self.k * iqr
            upper = q3 + self.k * iqr

        df = df.copy()
        df["is_anomaly"] = (values < lower) | (values > upper)

        return df

