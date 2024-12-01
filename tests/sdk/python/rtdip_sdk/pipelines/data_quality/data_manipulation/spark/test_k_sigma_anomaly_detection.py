from pyspark.sql import SparkSession

from src.sdk.python.rtdip_sdk.pipelines.data_quality.data_manipulation.spark.k_sigma_anomaly_detection import (
    KSigmaAnomalyDetection,
)

# Normal data mean=10 stddev=5 + 3 anomalies
# fmt: off
normal_input_values = [ 5.19811497,  8.34437927,  3.62104032, 10.02819525,  6.1183447 ,
                20.10067378, 10.32313075, 14.090119  , 21.43078927,  2.76624332,
                10.84089416,  1.90722629, 11.19750641, 13.70925639,  5.61011921,
                4.50072694, 13.79440311, 13.30173747,  7.07183589, 12.79853139, 100]

normal_expected_values = [ 5.19811497,  8.34437927,  3.62104032, 10.02819525,  6.1183447 ,
                   20.10067378, 10.32313075, 14.090119  , 21.43078927,  2.76624332,
                   10.84089416,  1.90722629, 11.19750641, 13.70925639,  5.61011921,
                   4.50072694, 13.79440311, 13.30173747,  7.07183589, 12.79853139]
# fmt: on

# These values are tricky for the mean method, as the anomaly has a big effect on the mean
input_values = [1, 2, 3, 4, 20]
expected_values = [1, 2, 3, 4]


def test_filter_with_mean(spark_session: SparkSession):
    # Test with normal data
    normal_input_df = spark_session.createDataFrame(
        [(float(num),) for num in normal_input_values], schema=["value"]
    )
    normal_expected_df = spark_session.createDataFrame(
        [(float(num),) for num in normal_expected_values], schema=["value"]
    )

    normal_filtered_df = KSigmaAnomalyDetection(
        spark_session,
        normal_input_df,
        column_names=["value"],
        k_value=3,
        use_median=False,
    ).filter()

    assert normal_expected_df.collect() == normal_filtered_df.collect()

    # Test with data that has an anomaly that shifts the mean significantly
    input_df = spark_session.createDataFrame(
        [(float(num),) for num in input_values], schema=["value"]
    )
    expected_df = spark_session.createDataFrame(
        [(float(num),) for num in expected_values], schema=["value"]
    )

    filtered_df = KSigmaAnomalyDetection(
        spark_session, input_df, column_names=["value"], k_value=3, use_median=False
    ).filter()

    assert expected_df.collect() != filtered_df.collect()


def test_filter_with_median(spark_session: SparkSession):
    # Test with normal data
    normal_input_df = spark_session.createDataFrame(
        [(float(num),) for num in normal_input_values], schema=["value"]
    )
    normal_expected_df = spark_session.createDataFrame(
        [(float(num),) for num in normal_expected_values], schema=["value"]
    )

    normal_filtered_df = KSigmaAnomalyDetection(
        spark_session,
        normal_input_df,
        column_names=["value"],
        k_value=3,
        use_median=True,
    ).filter()

    normal_expected_df.show()
    normal_filtered_df.show()

    assert normal_expected_df.collect() == normal_filtered_df.collect()

    # Test with data that has an anomaly that shifts the mean significantly
    input_df = spark_session.createDataFrame(
        [(float(num),) for num in input_values], schema=["value"]
    )
    expected_df = spark_session.createDataFrame(
        [(float(num),) for num in expected_values], schema=["value"]
    )

    filtered_df = KSigmaAnomalyDetection(
        spark_session, input_df, column_names=["value"], k_value=3, use_median=True
    ).filter()

    assert expected_df.collect() == filtered_df.collect()
