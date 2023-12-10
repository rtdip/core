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

from pyspark.sql import DataFrame, SparkSession, functions as F

from ..base_raw_to_mdm import BaseRawToMDMTransformer
from ...._pipeline_utils.iso import ERCOT_SCHEMA, melt
from .....data_models.timeseries import SeriesType, ModelType, ValueType


class ERCOTToMDMTransformer(BaseRawToMDMTransformer):
    """
    Converts ERCOT Raw data into Meters Data Model.

    Please check the BaseRawToMDMTransformer for the required arguments and methods.

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.transformers import ERCOTToMDMTransformer
    from rtdip_sdk.pipelines.utilities import SparkSessionUtility

    # Not required if using Databricks
    spark = SparkSessionUtility(config={}).execute()

    ercot_to_mdm_transformer = ERCOTToMDMTransformer(
        spark=spark,
        data=df,
        output_type="usage",
        name=None,
        description=None,
        value_type=None,
        version=None,
        series_id=None,
        series_parent_id=None
    )

    result = ercot_to_mdm_transformer.transform()
    ```
    """

    spark: SparkSession
    data: DataFrame
    input_schema = ERCOT_SCHEMA
    uid_col = "variable"
    series_id_col = "'series_std_001'"
    timestamp_col = "to_utc_timestamp(StartTime, 'America/Chicago')"
    interval_timestamp_col = "Timestamp + INTERVAL 1 HOURS"
    value_col = "value"
    series_parent_id_col = "'series_parent_std_001'"
    name_col = "'ERCOT API'"
    uom_col = "'mwh'"
    description_col = "'ERCOT data pulled from ERCOT ISO API'"
    timestamp_start_col = "StartTime"
    timestamp_end_col = "StartTime + INTERVAL 1 HOURS"
    time_zone_col = "'America/Chicago'"
    version_col = "'1'"
    series_type = SeriesType.Hour
    model_type = ModelType.Default
    value_type = ValueType.Usage
    properties_col = "null"

    def _pre_process(self) -> DataFrame:
        df: DataFrame = super(ERCOTToMDMTransformer, self)._pre_process()
        df = melt(
            df,
            id_vars=["Date", "HourEnding", "DstFlag"],
            value_vars=[
                "Coast",
                "East",
                "FarWest",
                "North",
                "NorthCentral",
                "SouthCentral",
                "Southern",
                "West",
                "SystemTotal",
            ],
        )
        df = df.withColumn(
            "StartTime",
            F.expr(
                "Date + MAKE_INTERVAL(0,0,0,0,cast(split(HourEnding,':')[0] as integer),0,0)"
            ),
        )
        return df
