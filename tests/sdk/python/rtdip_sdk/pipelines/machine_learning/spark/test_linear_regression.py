# Copyright 2022 RTDIP
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
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    FloatType,
)
from datetime import datetime
from src.sdk.python.rtdip_sdk.pipelines.machine_learning.spark.linear_regression import (
    LinearRegression,
)
from src.sdk.python.rtdip_sdk.pipelines.transformers.spark.machine_learning.columns_to_vector import (
    ColumnsToVector,
)
from src.sdk.python.rtdip_sdk.pipelines.transformers.spark.machine_learning.polynomial_features import (
    PolynomialFeatures,
)


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder.master("local[*]")
        .appName("Linear Regression Unit Test")
        .getOrCreate()
    )


@pytest.fixture(scope="function")
def sample_data(spark):
    schema = StructType(
        [
            StructField("TagName", StringType(), True),
            StructField("EventTime", TimestampType(), True),
            StructField("Status", StringType(), True),
            StructField("Value", FloatType(), True),
        ]
    )

    data = [
        (
            "A2PS64V0J.:ZUX09R",
            datetime.strptime("2024-01-02 20:03:46.000", "%Y-%m-%d %H:%M:%S.%f"),
            "Good",
            0.3400000035762787,
        ),
        (
            "A2PS64V0J.:ZUX09R",
            datetime.strptime("2024-01-02 16:00:12.000", "%Y-%m-%d %H:%M:%S.%f"),
            "Good",
            0.15000000596046448,
        ),
        (
            "A2PS64V0J.:ZUX09R",
            datetime.strptime("2024-01-02 11:56:42.000", "%Y-%m-%d %H:%M:%S.%f"),
            "Good",
            0.12999999523162842,
        ),
        (
            "A2PS64V0J.:ZUX09R",
            datetime.strptime("2024-01-02 07:53:11.000", "%Y-%m-%d %H:%M:%S.%f"),
            "Good",
            0.11999999731779099,
        ),
        (
            "A2PS64V0J.:ZUX09R",
            datetime.strptime("2024-01-02 03:49:45.000", "%Y-%m-%d %H:%M:%S.%f"),
            "Good",
            0.12999999523162842,
        ),
        (
            "-4O7LSSAM_3EA02:2GT7E02I_R_MP",
            datetime.strptime("2024-01-02 20:09:58.053", "%Y-%m-%d %H:%M:%S.%f"),
            "Good",
            7107.82080078125,
        ),
        (
            "_LT2EPL-9PM0.OROTENV3:",
            datetime.strptime("2024-01-02 12:27:10.518", "%Y-%m-%d %H:%M:%S.%f"),
            "Good",
            19407.0,
        ),
        (
            "_LT2EPL-9PM0.OROTENV3:",
            datetime.strptime("2024-01-02 05:23:10.143", "%Y-%m-%d %H:%M:%S.%f"),
            "Good",
            19403.0,
        ),
        (
            "_LT2EPL-9PM0.OROTENV3:",
            datetime.strptime("2024-01-02 01:31:10.086", "%Y-%m-%d %H:%M:%S.%f"),
            "Good",
            19399.0,
        ),
        (
            "1N325T3MTOR-P0L29:9.T0",
            datetime.strptime("2024-01-02 23:41:10.358", "%Y-%m-%d %H:%M:%S.%f"),
            "Good",
            19376.0,
        ),
        (
            "TT33-01M9Z2L9:P20.AIRO5N",
            datetime.strptime("2024-01-02 18:09:10.488", "%Y-%m-%d %H:%M:%S.%f"),
            "Good",
            19375.0,
        ),
        (
            "TT33-01M9Z2L9:P20.AIRO5N",
            datetime.strptime("2024-01-02 16:15:10.492", "%Y-%m-%d %H:%M:%S.%f"),
            "Good",
            19376.0,
        ),
        (
            "TT33-01M9Z2L9:P20.AIRO5N",
            datetime.strptime("2024-01-02 06:51:10.077", "%Y-%m-%d %H:%M:%S.%f"),
            "Good",
            19403.0,
        ),
        (
            "O:05RI0.2T2M6STN6_PP-I165AT",
            datetime.strptime("2024-01-02 07:42:24.227", "%Y-%m-%d %H:%M:%S.%f"),
            "Good",
            6.55859375,
        ),
        (
            "-4O7LSSAM_3EA02:2GT7E02I_R_MP",
            datetime.strptime("2024-01-02 06:08:23.777", "%Y-%m-%d %H:%M:%S.%f"),
            "Good",
            5921.5498046875,
        ),
        (
            "-4O7LSSAM_3EA02:2GT7E02I_R_MP",
            datetime.strptime("2024-01-02 05:14:10.896", "%Y-%m-%d %H:%M:%S.%f"),
            "Good",
            5838.216796875,
        ),
        (
            "-4O7LSSAM_3EA02:2GT7E02I_R_MP",
            datetime.strptime("2024-01-02 01:37:10.967", "%Y-%m-%d %H:%M:%S.%f"),
            "Good",
            5607.82568359375,
        ),
        (
            "-4O7LSSAM_3EA02:2GT7E02I_R_MP",
            datetime.strptime("2024-01-02 00:26:53.449", "%Y-%m-%d %H:%M:%S.%f"),
            "Good",
            5563.7080078125,
        ),
    ]

    return spark.createDataFrame(data, schema=schema)


# Test cases
def test_cols_to_vector(sample_data):
    df = sample_data
    # Pass the DataFrame to ColumnsToVector
    cols_to_vector = ColumnsToVector(df=df, input_cols=["Value"], output_col="features")
    transformed_df = cols_to_vector.transform()

    assert "features" in transformed_df.columns
    transformed_df.show()


def test_polynomial_features(sample_data):
    df = sample_data
    # Convert 'Value' to a vector using ColumnsToVector
    cols_to_vector = ColumnsToVector(df=df, input_cols=["Value"], output_col="features")
    vectorized_df = cols_to_vector.transform()

    polynomial_features = PolynomialFeatures(
        df=vectorized_df,
        input_col="features",
        output_col="poly_features",
        poly_degree=2,
    )
    transformed_df = polynomial_features.transform()
    assert "poly_features" in transformed_df.columns
    transformed_df.show()


def test_linear_regression(sample_data):
    df = sample_data
    # Use ColumnsToVector to assemble features into a single vector column
    cols_to_vector = ColumnsToVector(df=df, input_cols=["Value"], output_col="features")
    df = cols_to_vector.transform()
    linear_regression = LinearRegression(
        df, features_col="features", label_col="Value", prediction_col="prediction"
    )

    # Split data and train
    train_df, test_df = linear_regression.split_data(train_ratio=0.8)
    model = linear_regression.train(train_df)

    # Check prediction column
    predictions = model.predict(test_df)
    assert "prediction" in predictions.columns

    # Evaluate the model
    rmse, r2 = linear_regression.evaluate(predictions)

    # Check if RMSE and R2 values are non-negative
    assert rmse >= 0
    assert 0 <= r2 <= 1

    predictions.show()
    print(f"RMSE: {rmse}, R2: {r2}")
