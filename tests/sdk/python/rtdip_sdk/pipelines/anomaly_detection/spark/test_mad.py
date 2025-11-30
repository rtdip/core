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

from src.sdk.python.rtdip_sdk.pipelines.anomaly_detection.spark.mad_anomaly_detection import (
    MadAnomalyDetection,
    MadAnomalyDetectionRollingWindow,
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


def test_mad_anomaly_detection(spark_dataframe_with_anomalies):
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


def test_mad_anomaly_detection_rolling_window(spark_dataframe_with_anomalies_big):
    # Using a smaller window size to detect anomalies in the larger dataset
    mad_detector = MadAnomalyDetectionRollingWindow(window_size=15)
    result_df = mad_detector.detect(spark_dataframe_with_anomalies_big)

    # assert all 3 anomalies are detected
    assert result_df.count() == 3

    # check that the detected anomalies are the expected ones
    assert result_df.collect()[0]["value"] == 0.0
    assert result_df.collect()[1]["value"] == 30.0
    assert result_df.collect()[2]["value"] == 40.0
