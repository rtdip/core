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
from enum import Enum
from typing import List

import pandas as pd
from pandas import DataFrame
from pyspark.sql import DataFrame as PySparkDataFrame, SparkSession, functions as F
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StringType, TimestampType, FloatType, NumericType, StructField, StructType
from statsmodels.tsa.arima.model import ARIMA, ARIMAResults
from pmdarima import auto_arima
import numpy as np

from ...interfaces import DataManipulationBaseInterface
from ....input_validator import InputValidator
from ....._pipeline_utils.spark import split_by_source, PROCESS_DATA_MODEL_EVENT_SCHEMA
from ......_sdk_utils.pandas import _prepare_pandas_to_convert_to_spark
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import (
    Libraries,
    SystemType,
)


class ArimaPrediction(DataManipulationBaseInterface, InputValidator):
    """
    Extends the timeseries data in given DataFrame with forecasted values from an ARIMA model. Can be optionally set
    to use auto_arima, which operates a bit like a grid search, in that it tries various sets of p and q (also P and Q
    for seasonal models) parameters, selecting the model that minimizes the AIC.
    It forecasts a value column of the given time series dataframe based on the historical data points and constructs
    full entries based on the preceding timestamps. It is advised to place this step after the missing value imputation
    to prevent learning on dirty data.

    ARIMA-Specific parameters can be viewed at the following statsmodels documentation page:
        https://www.statsmodels.org/dev/generated/statsmodels.tsa.arima.model.ARIMA.html

    Example
    -------
    ```python
        df = pandas.DataFrame()

    numpy.random.seed(0)
    arr_len = 250
    h_a_l = int(arr_len / 2)
    df['Value'] = np.random.rand(arr_len) + np.sin(np.linspace(0, arr_len / 10, num=arr_len))
    df['Value2'] = np.random.rand(arr_len) + np.cos(np.linspace(0, arr_len / 2, num=arr_len)) + 5
    df['index'] = np.asarray(pandas.date_range(start='1/1/2024', end='2/1/2024', periods=arr_len))
    df = df.set_index(pd.DatetimeIndex(df['index']))

    learn_df = df.head(h_a_l)

    # plt.plot(df['Value'])
    # plt.show()

    input_df = spark_session.createDataFrame(
            learn_df,
            ['Value', 'Value2', 'index'],
    )
    arima_comp = ArimaPrediction(input_df, column_name='Value', number_of_data_points_to_analyze=h_a_l, number_of_data_points_to_predict=h_a_l,
                         order=(3,0,0), seasonal_order=(3,0,0,62))
    forecasted_df = arima_comp.filter()
    ```

    Parameters:
        past_data (DataFrame): PySpark DataFrame to extend
        column_name (str): Name of the column to be extended
        timestamp_column_name (str): Name of the column containing timestamps
            external_regressor_column_names (List[str]): Names of the columns with data to use for prediction, but not extend
        number_of_data_points_to_predict (int): Amount of most recent rows used to create the model
        number_of_data_points_to_analyze (int): Amount of rows to predict with the model
        order (tuple): ARIMA-Specific setting
        seasonal_order (tuple): ARIMA-Specific setting
        trend (str): ARIMA-Specific setting
        enforce_stationarity (bool): ARIMA-Specific setting
        enforce_invertibility (bool): ARIMA-Specific setting
        concentrate_scale (bool): ARIMA-Specific setting
        trend_offset (int): ARIMA-Specific setting
        missing (str): ARIMA-Specific setting
        arima_auto (bool) pmdarima-specific setting to enable auto_arima
    """

    df: PySparkDataFrame
    spark_session: SparkSession

    column_to_predict: str
    rows_to_predict: int
    rows_to_analyze: int

    value_name: str
    timestamp_name: str
    source_name: str
    external_regressor_names: List[str]

    class InputStyle(Enum):
        COLUMN_BASED = 1
        SOURCE_BASED = 2

    def __init__(
        self,
        past_data: PySparkDataFrame,
        past_data_style : InputStyle = InputStyle.COLUMN_BASED,
        to_extend_name: str = None, #either source or column
        value_name: str = None,
        timestamp_name: str = None,
        source_name: str = None,
        status_name: str = None,
        external_regressor_names: List[str] = None,
        number_of_data_points_to_predict: int = 50,
        number_of_data_points_to_analyze: int = None,
        order: tuple = (0, 0, 0),
        seasonal_order: tuple = (0, 0, 0, 0),
        trend="c",
        enforce_stationarity: bool = True,
        enforce_invertibility: bool = True,
        concentrate_scale: bool = False,
        trend_offset: int = 1,
        missing: str = "None",
        arima_auto: bool = False,
    ) -> None:
        if not value_name in past_data.columns:
            raise ValueError("{} not found in the DataFrame.".format(value_name))

        self.past_data = past_data
        self.past_data_style = past_data_style
        # Convert source-based datafram to column-based
        if past_data_style == self.InputStyle.COLUMN_BASED:
            self.df = past_data
        elif past_data_style == self.InputStyle.SOURCE_BASED:
            self.df = past_data.groupby(timestamp_name).pivot(source_name).agg(F.first(value_name))

        self.spark_session = past_data.sparkSession
        self.column_to_predict = to_extend_name
        self.rows_to_predict = number_of_data_points_to_predict
        self.rows_to_analyze = number_of_data_points_to_analyze or past_data.count()
        self.arima_auto = arima_auto
        self.order = order
        self.seasonal_order = seasonal_order
        self.trend = trend
        self.enforce_stationarity = enforce_stationarity
        self.enforce_invertibility = enforce_invertibility
        self.concentrate_scale = concentrate_scale
        self.trend_offset = trend_offset
        self.missing = missing
        self.value_name = value_name
        self.timestamp_name = timestamp_name
        self.source_name = source_name
        self.external_regressor_names = external_regressor_names
        self.status_name = status_name

        if external_regressor_names is not None:
            # TODO: exog, e.g. external regressors / exogenic variables
            raise NotImplementedError(
                "Handling of external regressors is not implemented"
            )
        # input_data.index = self.df.loc[:, timestamp_column_name].sort_index(ascending=True).tail(number_of_data_points_to_analyze)
        # input_data.index = pd.DatetimeIndex(input_data.index).to_period()

    @staticmethod
    def system_type():
        """
        Attributes:
            SystemType (Environment): Requires PYSPARK
        """
        return SystemType.PYSPARK

    @staticmethod
    def libraries():
        libraries = Libraries()
        return libraries

    @staticmethod
    def settings() -> dict:
        return {}

    @staticmethod
    def _is_column_type(df, column_name, data_type):
        """
        Helper method for data type checking
        """
        type_ = df.schema[column_name]

        return isinstance(type_.dataType, data_type)

    def filter(self) -> PySparkDataFrame:
        """
        Forecasts a value column of a given time series dataframe based on the historical data points using ARIMA.

        Constructs full entries based on the preceding timestamps. It is advised to place this step after the missing
        value imputation to prevent learning on dirty data.

        Returns:
            DataFrame: A PySpark DataFrame with forcasted value entries on each source.
        """
        # expected_scheme = StructType(
        #    [
        #        StructField("TagName", StringType(), True),
        #        StructField("EventTime", TimestampType(), True),
        #        StructField("Status", StringType(), True),
        #        StructField("Value", NumericType(), True),
        #    ]
        #)

        # self.validate(expected_scheme)

        pd_df = self.df.toPandas()
        main_signal_df = pd_df[pd_df[self.column_to_predict].notna()]

        main_signal_df[self.timestamp_name] = pd.to_datetime(main_signal_df[self.timestamp_name]).astype("datetime64[ns]")
        #main_signal_df["Value"] = main_signal_df["Value"].astype("float64")
        #main_signal_df = main_signal_df.set_index("EventTime")

        input_data = main_signal_df[self.column_to_predict].sort_index(ascending=True).tail(self.rows_to_analyze)

        if self.arima_auto:
            # Default case: False
            auto_model = auto_arima(
                y=input_data,
                seasonal=any(self.seasonal_order),
                stepwise=True,
                suppress_warnings=True,
                trace=True,
                error_action="ignore",
                max_order=None,
            )

            order = auto_model.order
            seasonal_order = auto_model.seasonal_order
            trend = "c" if order[1] == 0 else "t"

        else:
            order = self.order
            seasonal_order = self.seasonal_order
            trend = self.trend

        source_model = ARIMA(
            endog=input_data,
            order=order,
            seasonal_order=seasonal_order,
            trend=trend,
            enforce_stationarity=self.enforce_stationarity,
            enforce_invertibility=self.enforce_invertibility,
            concentrate_scale=self.concentrate_scale,
            trend_offset=self.trend_offset,
            missing=self.missing,
        ).fit()

        forecast = source_model.forecast(steps=self.rows_to_predict)
        inferred_freq = pd.Timedelta(value=statistics.mode(np.diff(main_signal_df[self.timestamp_name].values)))

        pd_forecast_df = pd.DataFrame(
                {
                    self.timestamp_name: pd.date_range(start=main_signal_df[self.timestamp_name].max() + inferred_freq, periods=self.rows_to_predict, freq=inferred_freq),
                    self.column_to_predict: forecast
                }
            )

        pd_df = pd.concat([pd_df, pd_forecast_df])

        # Workaround needed for PySpark versions <3.4
        pd_df = _prepare_pandas_to_convert_to_spark(pd_df)

        if self.past_data_style == self.InputStyle.COLUMN_BASED:
            raise NotImplementedError(
                "Column-based export"
            )
        elif self.past_data_style == self.InputStyle.SOURCE_BASED:
            data_to_add = pd_forecast_df[[self.timestamp_name, self.column_to_predict]]
            data_to_add = data_to_add.rename(columns={self.timestamp_name: self.timestamp_name, self.column_to_predict: self.value_name})
            data_to_add[self.source_name] = self.column_to_predict

            pd_df_schema = StructType(
               [
                    StructField(self.source_name, StringType(), True),
                    StructField(self.timestamp_name, StringType(), True),
                    StructField(self.value_name, StringType(), True)
                ]
            )

            predicted_source_pyspark_dataframe = self.spark_session.createDataFrame(
                data_to_add, schema=pd_df_schema
            )

            if self.status_name is not None:
                predicted_source_pyspark_dataframe = predicted_source_pyspark_dataframe.withColumn(self.status_name, lit("Predicted"))

            return self.past_data.union(predicted_source_pyspark_dataframe)

    def _get_source_model(self, source_df) -> ARIMAResults:
        """
        Helper method to get the ARIMA model and calculate the best fitting model if auto_arima is enabled
        """
        pass


