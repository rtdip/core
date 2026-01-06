import pandas as pd

from .iqr_anomaly_detection import IQRAnomalyDetectionComponent
from .interfaces import IQRAnomalyDetectionConfig


class DecompositionIQRAnomalyDetectionComponent(
    IQRAnomalyDetectionComponent
):
    """
    IQR anomaly detection on decomposed time series.

    Expected input columns:
    - residual (default)
    - trend
    - seasonal
    """

    def __init__(self, config: IQRAnomalyDetectionConfig):
        super().__init__(config)
        self.input_component: str = config.get("input_component", "residual")

    def run(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Run anomaly detection on a selected decomposition component.
        """

        if self.input_component not in df.columns:
            raise ValueError(
                f"Column '{self.input_component}' not found in input DataFrame"
            )

        df = df.copy()
        df[self.value_column] = df[self.input_component]

        return super().run(df)

