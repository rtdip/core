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
from pyspark.sql import DataFrame as PySparkDataFrame, SparkSession
from statsmodels.tsa.arima.model import ARIMA, ARIMAResults

from ....interfaces import WranglerBaseInterface
from ....._pipeline_utils.models import Libraries, SystemType


class ArimaPrediction(WranglerBaseInterface):
    """
    Extends a column in given DataFrame with a ARIMA model.

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
    """

    model: ARIMA
    result: ARIMAResults
    df: DataFrame
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
    ) -> None:
        if not column_name in past_data.columns:
            raise ValueError("{} not found in the DataFrame.".format(column_name))

        self.df = past_data.toPandas()
        self.spark_session = past_data.sparkSession
        self.column_to_predict = column_name
        self.rows_to_predict = number_of_data_points_to_predict
        self.rows_to_analyze = number_of_data_points_to_analyze or past_data.count()
        input_data = (
            self.df.loc[:, column_name]
            .sort_index(ascending=True)
            .tail(number_of_data_points_to_analyze)
        )
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
        self.model = ARIMA(
            endog=input_data,
            order=order,
            seasonal_order=seasonal_order,
            trend=trend,
            enforce_stationarity=enforce_stationarity,
            enforce_invertibility=enforce_invertibility,
            concentrate_scale=concentrate_scale,
            trend_offset=trend_offset,
            missing=missing,
        )
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
        Predicts value to predict and extends the in the constructor inputted Dataframe.

        Other columns will be filled with NaN other similar None values.

        Returns:
            DataFrame: A PySpark DataFrame with extended index and filled column_to_predict.
        """
        prediction_start = self.df.index.max() + 1
        prediction_end = self.df.index.max() + self.rows_to_predict
        prediction_series = self.result.predict(
            start=prediction_start, end=prediction_end
        ).rename(self.column_to_predict)
        extended_df = pd.concat([self.df, prediction_series.to_frame()])
        # Workaround needed for PySpark versions <3.4
        if not hasattr(extended_df, "iteritems"):
            extended_df.iteritems = extended_df.items
        return self.spark_session.createDataFrame(extended_df)
