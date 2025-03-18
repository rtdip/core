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
import os
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    FloatType,
)
from datetime import datetime
from src.sdk.python.rtdip_sdk.pipelines.forecasting.spark.k_nearest_neighbors import (
    KNearestNeighbors,
)
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.sql.functions import col

# Schema definition (same as template)
SCHEMA = StructType(
    [
        StructField("TagName", StringType(), True),
        StructField("EventTime", TimestampType(), True),
        StructField("Status", StringType(), True),
        StructField("Value", FloatType(), True),
    ]
)


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder.master("local[*]").appName("KNN Unit Test").getOrCreate()
    )


@pytest.fixture(scope="function")
def sample_data(spark):
    # Using similar data structure as template but with more varied values
    data = [
        (
            "TAG1",
            datetime.strptime("2024-01-02 20:03:46.000", "%Y-%m-%d %H:%M:%S.%f"),
            "Good",
            0.34,
        ),
        (
            "TAG1",
            datetime.strptime("2024-01-02 20:04:46.000", "%Y-%m-%d %H:%M:%S.%f"),
            "Good",
            0.35,
        ),
        (
            "TAG2",
            datetime.strptime("2024-01-02 20:05:46.000", "%Y-%m-%d %H:%M:%S.%f"),
            "Good",
            0.45,
        ),
        (
            "TAG2",
            datetime.strptime("2024-01-02 20:06:46.000", "%Y-%m-%d %H:%M:%S.%f"),
            "Bad",
            0.55,
        ),
    ]
    return spark.createDataFrame(data, schema=SCHEMA)


@pytest.fixture(scope="function")
def prepared_data(sample_data):
    # Convert categorical variables, Index TagName and Status
    tag_indexer = StringIndexer(inputCol="TagName", outputCol="TagIndex")
    status_indexer = StringIndexer(inputCol="Status", outputCol="StatusIndex")

    df = tag_indexer.fit(sample_data).transform(sample_data)
    df = status_indexer.fit(df).transform(df)

    assembler = VectorAssembler(
        inputCols=["TagIndex", "StatusIndex", "Value"], outputCol="raw_features"
    )
    df = assembler.transform(df)

    scaler = StandardScaler(
        inputCol="raw_features", outputCol="features", withStd=True, withMean=True
    )
    return scaler.fit(df).transform(df)


def test_knn_initialization(prepared_data):
    """Test KNN initialization with various parameters"""
    # Test valid initialization
    knn = KNearestNeighbors(
        features_col="features",
        label_col="Value",
        timestamp_col="EventTime",
        k=3,
        weighted=True,
        distance_metric="combined",
    )
    assert knn.k == 3
    assert knn.weighted is True

    # Test invalid distance metric
    with pytest.raises(ValueError):
        KNearestNeighbors(
            features_col="features",
            label_col="Value",
            distance_metric="invalid_metric",
        )

    # Test missing timestamp column for temporal distance
    with pytest.raises(ValueError):
        KNearestNeighbors(
            features_col="features",
            label_col="Value",
            # timestamp_col is compulsory for temporal distance
            distance_metric="temporal",
        )


def test_data_splitting(prepared_data):
    """Test the data splitting functionality"""
    knn = KNearestNeighbors(
        features_col="features",
        label_col="Value",
        timestamp_col="EventTime",
    )

    train_df, test_df = prepared_data.randomSplit([0.8, 0.2], seed=42)

    assert train_df.count() + test_df.count() == prepared_data.count()
    assert train_df.count() > 0
    assert test_df.count() > 0


def test_model_training(prepared_data):
    """Test model training functionality"""
    knn = KNearestNeighbors(
        features_col="features",
        label_col="Value",
        timestamp_col="EventTime",
    )

    train_df, _ = prepared_data.randomSplit([0.8, 0.2], seed=42)
    trained_model = knn.train(train_df)

    assert trained_model is not None
    assert trained_model.train_features is not None
    assert trained_model.train_labels is not None


def test_predictions(prepared_data):
    """Test prediction functionality"""
    knn = KNearestNeighbors(
        features_col="features",
        label_col="Value",
        timestamp_col="EventTime",
        weighted=True,
    )

    train_df, test_df = prepared_data.randomSplit([0.8, 0.2], seed=42)
    knn.train(train_df)
    predictions = knn.predict(test_df)

    assert "prediction" in predictions.columns
    assert predictions.count() > 0
    assert all(pred is not None for pred in predictions.select("prediction").collect())


def test_temporal_distance(prepared_data):
    """Test temporal distance calculation"""
    knn = KNearestNeighbors(
        features_col="features",
        label_col="Value",
        timestamp_col="EventTime",
        distance_metric="temporal",
    )

    train_df, test_df = prepared_data.randomSplit([0.8, 0.2], seed=42)
    knn.train(train_df)
    predictions = knn.predict(test_df)

    assert predictions.count() > 0
    assert "prediction" in predictions.columns


def test_combined_distance(prepared_data):
    """Test combined distance calculation"""
    knn = KNearestNeighbors(
        features_col="features",
        label_col="Value",
        timestamp_col="EventTime",
        distance_metric="combined",
        temporal_weight=0.5,
    )

    train_df, test_df = prepared_data.randomSplit([0.8, 0.2], seed=42)
    knn.train(train_df)
    predictions = knn.predict(test_df)

    assert predictions.count() > 0
    assert "prediction" in predictions.columns


def test_invalid_data_handling(spark):
    """Test handling of invalid data"""
    invalid_data = [
        ("TAG1", "invalid_date", "Good", "invalid_value"),
        ("TAG1", "2024-01-02 20:03:46.000", "Good", "NaN"),
        ("TAG2", "2024-01-02 20:03:46.000", None, 123.45),
    ]

    schema = StructType(
        [
            StructField("TagName", StringType(), True),
            StructField("EventTime", StringType(), True),
            StructField("Status", StringType(), True),
            StructField("Value", StringType(), True),
        ]
    )

    df = spark.createDataFrame(invalid_data, schema=schema)

    try:
        df = df.withColumn("Value", col("Value").cast(FloatType()))
        invalid_rows = df.filter(col("Value").isNull())
        valid_rows = df.filter(col("Value").isNotNull())

        assert invalid_rows.count() > 0
        assert valid_rows.count() > 0
    except Exception as e:
        pytest.fail(f"Unexpected error during invalid data handling: {e}")


def test_large_dataset(spark):
    """Test KNN on a larger dataset"""
    base_path = os.path.dirname(__file__)
    file_path = os.path.join(base_path, "../../data_quality/test_data.csv")

    try:
        df = spark.read.option("header", "true").csv(file_path)
        df = df.withColumn("Value", col("Value").cast(FloatType()))
        df = df.withColumn("EventTime", col("EventTime").cast(TimestampType()))

        prepared_df = prepare_data_for_knn(df)

        knn = KNearestNeighbors(
            features_col="features",
            label_col="Value",
            timestamp_col="EventTime",
        )

        train_df, test_df = prepared_df.randomSplit([0.8, 0.2], seed=42)
        knn.train(train_df)
        predictions = knn.predict(test_df)

        assert predictions.count() > 0
        assert "prediction" in predictions.columns
    except Exception as e:
        pytest.fail(f"Failed to process large dataset: {e}")


def prepare_data_for_knn(df):
    """Helper function to prepare data for KNN"""

    # Convert categorical variables
    indexers = [
        StringIndexer(inputCol=col, outputCol=f"{col}Index")
        for col in ["TagName", "Status"]
        if col in df.columns
    ]

    for indexer in indexers:
        df = indexer.fit(df).transform(df)

    # Create feature vector
    numeric_cols = [col for col in df.columns if df.schema[col].dataType == FloatType()]
    index_cols = [col for col in df.columns if col.endswith("Index")]
    feature_cols = numeric_cols + index_cols

    assembler = VectorAssembler(inputCols=feature_cols, outputCol="raw_features")
    df = assembler.transform(df)

    # Scale features
    scaler = StandardScaler(
        inputCol="raw_features", outputCol="features", withStd=True, withMean=True
    )
    return scaler.fit(df).transform(df)
