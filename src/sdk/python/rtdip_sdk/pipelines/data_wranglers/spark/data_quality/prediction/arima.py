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
import pyspark.pandas
from pandas import DataFrame
from pyspark.sql.functions import desc
from pyspark.sql import DataFrame as PySparkDataFrame, SparkSession
import statsmodels.api as sm
from statsmodels.tsa.arima.model import ARIMA, ARIMAResults

from ....interfaces import WranglerBaseInterface
from ....._pipeline_utils.models import Libraries, SystemType


class ArimaPrediction(WranglerBaseInterface):
    """
    Oneliner.

    Example
    --------
    ```python

    ```

    Parameters:
        df (DataFrame): PySpark DataFrame to be converted
    """

    model: ARIMA
    result: ARIMAResults
    df: DataFrame
    spark_session: SparkSession

    column_to_predict : str
    rows_to_predict : int
    rows_to_analyze : int

    def __init__(self, past_data: PySparkDataFrame, column_name: str,
                 number_of_data_points_to_predict: int = 50, number_of_data_points_to_analyze: int = None,
                 order: tuple = (0,0,0), seasonal_order: tuple = (0,0,0,0), trend = "c", enforce_stationarity: bool = True,
                 enforce_invertibility: bool = True, concentrate_scale: bool = False, trend_offset: int = 1,
                 dates = None, freq=None, missing: str = "None") -> None:
        #TODO: Adds support for datetime
        #TODO: exog
        self.df = past_data.toPandas()
        self.spark_session = past_data.sparkSession
        self.column_to_predict = column_name
        self.rows_to_predict = number_of_data_points_to_predict
        self.rows_to_analyze = number_of_data_points_to_analyze or past_data.count()
        input_data = self.df.loc[:, column_name].sort_index(ascending=True).tail(number_of_data_points_to_analyze)
        #TODO: How to choose parameters better?
        self.model = ARIMA(endog=input_data, order=order, seasonal_order=seasonal_order, trend=trend, enforce_stationarity=enforce_stationarity,
                           enforce_invertibility=enforce_invertibility, concentrate_scale=concentrate_scale, trend_offset=trend_offset,
                           dates=dates, freq=freq, missing=missing)
        self.result = self.model.fit()



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

    def filter(self) -> PySparkDataFrame:
        """
        Returns:
            DataFrame: A cleansed PySpark DataFrame from all the duplicates.
        """
        prediction_start = self.df.index.max() + 1
        prediction_end = self.df.index.max() + self.rows_to_predict
        prediction_series = self.result.predict(start=prediction_start, end=prediction_end).rename(self.column_to_predict)
        # extended_pydf = self.df.alias("extended_pydf")
        extended_df = pd.concat([self.df, prediction_series.to_frame()])
        # extended_df[self.column_to_predict] = pd.concat([self.df[self.column_to_predict], prediction_series], axis=0)
        return self.spark_session.createDataFrame(extended_df)
        #TODO: Conversion from Dataframe to PySparkDataframe
