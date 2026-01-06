from typing import TypedDict, Optional


class IQRAnomalyDetectionConfig(TypedDict, total=False):
    """
    Configuration schema for IQR anomaly detection components.
    """

    # IQR sensitivity factor
    k: float

    # Rolling window size (None = global IQR)
    window: Optional[int]

    # Column names
    value_column: str
    time_column: str

    # Used only for decomposition-based component
    input_component: str

