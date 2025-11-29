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
