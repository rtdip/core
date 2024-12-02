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
from typing import List

import pandas as pd
from pandas import DataFrame
from pyspark.sql import DataFrame as PySparkDataFrame, SparkSession, functions as F
from pyspark.sql.types import StringType, TimestampType, FloatType
from pyspark.sql.functions import col
from statsmodels.tsa.arima.model import ARIMA, ARIMAResults
from pmdarima import auto_arima

from ...interfaces import DataManipulationBaseInterface
from ....input_validator import InputValidator
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

    def __init__(
        self,
        past_data: PySparkDataFrame,
        column_name: str,
        timestamp_column_name: str = None,
        external_regressor_column_names: List[str] = None,
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
        if not column_name in past_data.columns:
            raise ValueError("{} not found in the DataFrame.".format(column_name))

        self.df = past_data
        self.spark_session = past_data.sparkSession
        self.column_to_predict = column_name
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

        if external_regressor_column_names is not None:
            # TODO: exog, e.g. external regressors / exogenic variables
            raise NotImplementedError(
                "Handling of external regressors is not implemented"
            )
        if timestamp_column_name is not None:
            # TODO: Adds support for datetime
            raise NotImplementedError("Timestamp Indexing not implemented")
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
        if not self._is_column_type(self.df, "EventTime", TimestampType):
            if self._is_column_type(self.df, "EventTime", StringType):
                # Attempt to parse the first format, then fallback to the second
                self.df = self.df.withColumn(
                    "EventTime",
                    F.coalesce(
                        F.to_timestamp("EventTime", "yyyy-MM-dd HH:mm:ss.SSS"),
                        F.to_timestamp("EventTime", "yyyy-MM-dd HH:mm:ss"),
                        F.to_timestamp("EventTime", "dd.MM.yyyy HH:mm:ss"),
                    ),
                )
        if not self._is_column_type(self.df, "Value", FloatType):
            self.df = self.df.withColumn("Value", self.df["Value"].cast(FloatType()))

        dfs_by_source = self._split_by_source()

        predicted_dfs: List[PySparkDataFrame] = []

        for source, df in dfs_by_source.items():
            if len(dfs_by_source) > 1:
                self.rows_to_analyze = df.count()
                self.rows_to_predict = int(df.count() / 2)

            base_df = df.toPandas()
            base_df["EventTime"] = pd.to_datetime(base_df["EventTime"])
            base_df["Value"] = base_df["Value"].astype("float64")
            base_df["EventTime"] = base_df["EventTime"].astype("datetime64[ns]")
            last_event_time = base_df["EventTime"].iloc[-1]
            second_last_event_time = base_df["EventTime"].iloc[-2]

            interval = last_event_time - second_last_event_time
            new_event_times = [
                last_event_time + (i * interval)
                for i in range(1, self.rows_to_predict + 1)
            ]

            source_model = self._get_source_model(base_df)

            forecast = source_model.get_forecast(steps=self.rows_to_predict)
            prediction_series = forecast.predicted_mean

            if len(prediction_series) != len(new_event_times):
                min_length = min(len(prediction_series), len(new_event_times))
                prediction_series = prediction_series[:min_length]
                new_event_times = new_event_times[:min_length]

            predicted_df = pd.DataFrame(
                {
                    "TagName": [base_df["TagName"].iloc[0]] * len(new_event_times),
                    "EventTime": new_event_times,
                    "Status": ["Predicted"] * len(new_event_times),
                    "Value": prediction_series.values,
                }
            )
            predicted_df["EventTime"] = predicted_df["EventTime"].astype(
                "datetime64[ns]"
            )

            extended_df = pd.concat([base_df, predicted_df], ignore_index=True)

            # Workaround needed for PySpark versions <3.4
            if not hasattr(extended_df, "iteritems"):
                extended_df.iteritems = extended_df.items

            predicted_source_pyspark_dataframe = self.spark_session.createDataFrame(
                extended_df
            )
            predicted_dfs.append(predicted_source_pyspark_dataframe)

        result_df = predicted_dfs[0]
        for df in predicted_dfs[1:]:
            result_df = result_df.unionByName(df)

        return result_df

    def _get_source_model(self, source_df) -> ARIMAResults:
        """
        Helper method to get the ARIMA model and calculate the best fitting model if auto_arima is enabled
        """
        input_data = (
            source_df.loc[:, self.column_to_predict]
            .sort_index(ascending=True)
            .tail(self.rows_to_analyze)
        )

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

        model = ARIMA(
            endog=input_data,
            order=order,
            seasonal_order=seasonal_order,
            trend=trend,
            enforce_stationarity=self.enforce_stationarity,
            enforce_invertibility=self.enforce_invertibility,
            concentrate_scale=self.concentrate_scale,
            trend_offset=self.trend_offset,
            missing=self.missing,
        )

        return model.fit()

    def _split_by_source(self) -> dict:
        """
        Helper method to separate individual time series based on their source
        """
        tag_names = self.df.select("TagName").distinct().collect()
        tag_names = [row["TagName"] for row in tag_names]
        source_dict = {
            tag: self.df.filter(col("TagName") == tag).orderBy("EventTime")
            for tag in tag_names
        }

        return source_dict
