# Copyright 2025 RTDIP
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pytest

from src.sdk.python.rtdip_sdk.pipelines.anomaly_detection.spark.mad.mad_anomaly_detection import (
    GlobalMadScorer,
    RollingMadScorer,
    MadAnomalyDetection,
    DecompositionMadAnomalyDetection,
)


@pytest.fixture
def spark_dataframe_with_anomalies(spark_session):
    data = [
        (1, 10.0),
        (2, 12.0),
        (3, 10.5),
        (4, 11.0),
        (5, 30.0),  # Anomalous value
        (6, 10.2),
        (7, 9.8),
        (8, 10.1),
        (9, 10.3),
        (10, 10.0),
    ]
    columns = ["timestamp", "value"]
    return spark_session.createDataFrame(data, columns)


def test_mad_anomaly_detection_global(spark_dataframe_with_anomalies):
    mad_detector = MadAnomalyDetection()

    result_df = mad_detector.detect(spark_dataframe_with_anomalies)

    # direct anomaly count check
    assert result_df.count() == 1

    row = result_df.collect()[0]
    assert row["value"] == 30.0


@pytest.fixture
def spark_dataframe_with_anomalies_big(spark_session):
    data = [
        (1, 5.8),
        (2, 6.6),
        (3, 6.2),
        (4, 7.5),
        (5, 7.0),
        (6, 8.3),
        (7, 8.1),
        (8, 9.7),
        (9, 9.2),
        (10, 10.5),
        (11, 10.7),
        (12, 11.4),
        (13, 12.1),
        (14, 11.6),
        (15, 13.0),
        (16, 13.6),
        (17, 14.2),
        (18, 14.8),
        (19, 15.3),
        (20, 15.0),
        (21, 16.2),
        (22, 16.8),
        (23, 17.4),
        (24, 18.1),
        (25, 17.7),
        (26, 18.9),
        (27, 19.5),
        (28, 19.2),
        (29, 20.1),
        (30, 20.7),
        (31, 0.0),
        (32, 21.5),
        (33, 22.0),
        (34, 22.9),
        (35, 23.4),
        (36, 30.0),
        (37, 23.8),
        (38, 24.9),
        (39, 25.1),
        (40, 26.0),
        (41, 40.0),
        (42, 26.5),
        (43, 27.4),
        (44, 28.0),
        (45, 28.8),
        (46, 29.1),
        (47, 29.8),
        (48, 30.5),
        (49, 31.0),
        (50, 31.6),
    ]

    columns = ["timestamp", "value"]
    return spark_session.createDataFrame(data, columns)


def test_mad_anomaly_detection_rolling(spark_dataframe_with_anomalies_big):
    # Using a smaller window size to detect anomalies in the larger dataset
    scorer = RollingMadScorer(threshold=3.5, window_size=15)
    mad_detector = MadAnomalyDetection(scorer=scorer)
    result_df = mad_detector.detect(spark_dataframe_with_anomalies_big)

    # assert all 3 anomalies are detected
    assert result_df.count() == 3

    # check that the detected anomalies are the expected ones
    assert result_df.collect()[0]["value"] == 0.0
    assert result_df.collect()[1]["value"] == 30.0
    assert result_df.collect()[2]["value"] == 40.0


@pytest.fixture
def spark_dataframe_synthetic_stl(spark_session):
    import numpy as np
    import pandas as pd

    np.random.seed(42)

    n = 500
    period = 24

    timestamps = pd.date_range("2025-01-01", periods=n, freq="H")
    trend = 0.02 * np.arange(n)
    seasonal = 5 * np.sin(2 * np.pi * np.arange(n) / period)
    noise = 0.3 * np.random.randn(n)

    values = trend + seasonal + noise

    anomalies = [50, 120, 121, 350, 400]
    values[anomalies] += np.array([8, -10, 9, 7, -12])

    pdf = pd.DataFrame({"timestamp": timestamps, "value": values})

    return spark_session.createDataFrame(pdf)


@pytest.mark.parametrize(
    "decomposition, period, scorer",
    [
        ("stl", 24, GlobalMadScorer(threshold=3.5)),
        ("stl", 24, RollingMadScorer(threshold=3.5, window_size=30)),
        ("mstl", 24, GlobalMadScorer(threshold=3.5)),
        ("mstl", 24, RollingMadScorer(threshold=3.5, window_size=30)),
    ],
)
def test_decomposition_mad_anomaly_detection(
    spark_dataframe_synthetic_stl,
    decomposition,
    period,
    scorer,
):
    detector = DecompositionMadAnomalyDetection(
        scorer=scorer,
        decomposition=decomposition,
        period=period,
        timestamp_column="timestamp",
        value_column="value",
    )

    result_df = detector.detect(spark_dataframe_synthetic_stl)

    # Expect exactly 5 anomalies (synthetic definition)
    assert result_df.count() == 5

    detected_values = sorted(row["value"] for row in result_df.collect())

    # STL/MSTL removes seasonality + trend, residual spikes survive
    assert len(detected_values) == 5
    assert min(detected_values) < -5  # negative anomaly
    assert max(detected_values) > 10  # positive anomaly
