import pyspark.sql
from pyspark.sql import SparkSession

import sys
from pathlib import Path

current_file = Path(__file__).resolve()
project_root = current_file.parents[3]   # ../../..
sdk_path = project_root / "src" / "sdk" / "python"
sys.path.insert(0, str(sdk_path))
from rtdip_sdk.pipelines.forecasting.spark.autogluon_timeseries import AutoGluonTimeSeries


def load_scada_data(parquet_name="scada_prepro.parquet") -> pyspark.sql.DataFrame:
    spark = (
        SparkSession.builder
            .master("local[*]")
            .appName("SCADA-Forecasting")
            .config("spark.driver.memory", "8g")
            .config("spark.executor.memory", "8g")
            .config("spark.driver.maxResultSize", "2g")
            .config("spark.sql.shuffle.partitions", "50")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .getOrCreate()
    )

    script_dir = current_file.parent
    parquet_path = script_dir / parquet_name

    df = spark.read.parquet(str(parquet_path))

    return df


def train_model(df: pyspark.sql.DataFrame):
    ag_model = AutoGluonTimeSeries()
    train_df, test_df = ag_model.split_data(df)
    ag_model.train(train_df)
    eva = ag_model.evaluate(test_df)

    return {"model": ag_model, "eva": eva ,"test_df": test_df}


def main(parquet_name="scada_prepro.parquet"):
    df = load_scada_data(parquet_name)
    result_dict = train_model(df)
    print(result_dict["eva"])


if __name__ == "__main__":
    main()
