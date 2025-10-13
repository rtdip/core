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
import statistics
from typing import List, Tuple

from pyspark.sql import DataFrame as PySparkDataFrame, SparkSession, functions as F
from pmdarima import auto_arima

from .arima import ArimaPrediction


class ArimaAutoPrediction(ArimaPrediction):
    """
    A wrapper for ArimaPrediction which uses pmdarima auto_arima for data prediction.
    It selectively tries various sets of p and q (also P and Q for seasonal models) parameters and selects the model with the minimal AIC.

    Example
    -------
    ```python
    import numpy as np
    import matplotlib.pyplot as plt
    import numpy.random
    import pandas
    from pyspark.sql import SparkSession

    from rtdip_sdk.pipelines.data_quality.forecasting.spark.arima import ArimaPrediction

    import rtdip_sdk.pipelines._pipeline_utils.spark as spark_utils
    from rtdip_sdk.pipelines.data_quality.forecasting.spark.auto_arima import ArimaAutoPrediction

    spark_session = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
    df = pandas.DataFrame()

    numpy.random.seed(0)
    arr_len = 250
    h_a_l = int(arr_len / 2)
    df['Value'] = np.random.rand(arr_len) + np.sin(np.linspace(0, arr_len / 10, num=arr_len))
    df['Value2'] = np.random.rand(arr_len) + np.cos(np.linspace(0, arr_len / 2, num=arr_len)) + 5
    df['index'] = np.asarray(pandas.date_range(start='1/1/2024', end='2/1/2024', periods=arr_len))
    df = df.set_index(pandas.DatetimeIndex(df['index']))

    learn_df = df.head(h_a_l)

    # plt.plot(df['Value'])
    # plt.show()

    input_df = spark_session.createDataFrame(
            learn_df,
            ['Value', 'Value2', 'index'],
    )
    arima_comp = ArimaAutoPrediction(input_df, to_extend_name='Value', number_of_data_points_to_analyze=h_a_l, number_of_data_points_to_predict=h_a_l,
                         seasonal=True)
    forecasted_df = arima_comp.filter_data().toPandas()
    print('Done')
    ```

    Parameters:
        past_data (PySparkDataFrame): PySpark DataFrame which contains training data
        to_extend_name (str): Column or source to forecast on
        past_data_style (InputStyle): In which format is past_data formatted
        value_name (str): Name of column in source-based format, where values are stored
        timestamp_name (str): Name of column, where event timestamps are stored
        source_name (str): Name of column in source-based format, where source of events are stored
        status_name (str): Name of column in source-based format, where status of events are stored
        external_regressor_names (List[str]): Currently not working. Names of the columns with data to use for prediction, but not extend
        number_of_data_points_to_predict (int): Amount of points to forecast
        number_of_data_points_to_analyze (int): Amount of most recent points to train on
        seasonal (bool): Setting for AutoArima, is past_data seasonal?
        enforce_stationarity (bool): ARIMA-Specific setting
        enforce_invertibility (bool): ARIMA-Specific setting
        concentrate_scale (bool): ARIMA-Specific setting
        trend_offset (int): ARIMA-Specific setting
        missing (str): ARIMA-Specific setting
    """

    def __init__(
        self,
        past_data: PySparkDataFrame,
        past_data_style: ArimaPrediction.InputStyle = None,
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
        # Convert source-based dataframe to column-based if necessary
        self._initialize_self_df(
            past_data,
            past_data_style,
            source_name,
            status_name,
            timestamp_name,
            to_extend_name,
            value_name,
        )
        # Prepare Input data
        input_data = self.df.toPandas()
        input_data = input_data[input_data[to_extend_name].notna()].tail(
            number_of_data_points_to_analyze
        )[to_extend_name]

        auto_model = auto_arima(
            y=input_data,
            seasonal=seasonal,
            stepwise=True,
            suppress_warnings=True,
            trace=False,  # Set to true if to debug
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
            trend="c" if auto_model.order[1] == 0 else "t",
            enforce_stationarity=enforce_stationarity,
            enforce_invertibility=enforce_invertibility,
            concentrate_scale=concentrate_scale,
            trend_offset=trend_offset,
            missing=missing,
        )
