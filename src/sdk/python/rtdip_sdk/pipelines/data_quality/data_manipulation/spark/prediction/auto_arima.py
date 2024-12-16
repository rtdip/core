# Copyright 2024 RTDIP
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
import statistics
from typing import List, Tuple

from pyspark.sql import DataFrame as PySparkDataFrame, SparkSession, functions as F
from pmdarima import auto_arima

from .arima import ArimaPrediction


class ArimaAutoPrediction(ArimaPrediction):
    def __init__(
        self,
        past_data: PySparkDataFrame,
        past_data_style : ArimaPrediction.InputStyle = None,
        to_extend_name: str = None,
        value_name: str = None,
        timestamp_name: str = None,
        source_name: str = None,
        status_name: str = None,
        external_regressor_names: List[str] = None,
        number_of_data_points_to_predict: int = 50,
        number_of_data_points_to_analyze: int = None,
        seasonal: bool = False,
        enforce_stationarity: bool = True,
        enforce_invertibility: bool = True,
        concentrate_scale: bool = False,
        trend_offset: int = 1,
        missing: str = "None",
    ) -> None:
        # Convert source-based dataframe to column-based
        self._initialize_self_df(past_data, past_data_style, source_name, status_name, timestamp_name, to_extend_name,
                                 value_name)
        # Prepare Input data
        input_data = self.df.toPandas()
        input_data = input_data[input_data[to_extend_name].notna()].tail(number_of_data_points_to_analyze)[to_extend_name]

        auto_model = auto_arima(
            y=input_data,
            seasonal=seasonal,
            stepwise=True,
            suppress_warnings=True,
            trace=False, # Set to true if to debug
            error_action="ignore",
            max_order=None,
        )

        super().__init__(
            past_data=past_data,
            past_data_style=self.past_data_style,
            to_extend_name=to_extend_name,
            value_name=self.value_name,
            timestamp_name=self.timestamp_name,
            source_name=self.source_name,
            status_name=self.status_name,
            external_regressor_names=external_regressor_names,
            number_of_data_points_to_predict=number_of_data_points_to_predict,
            number_of_data_points_to_analyze=number_of_data_points_to_analyze,
            order=auto_model.order,
            seasonal_order=auto_model.seasonal_order,
            trend = "c" if auto_model.order[1] == 0 else "t",
            enforce_stationarity=enforce_stationarity,
            enforce_invertibility=enforce_invertibility,
            concentrate_scale=concentrate_scale,
            trend_offset=trend_offset,
            missing=missing
        )

