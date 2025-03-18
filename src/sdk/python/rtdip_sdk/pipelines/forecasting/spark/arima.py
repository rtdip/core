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
import copy
import statistics
from enum import Enum
from typing import List, Tuple

import pandas as pd
from pandas import DataFrame
from pyspark.sql import (
    DataFrame as PySparkDataFrame,
    SparkSession,
    functions as F,
    DataFrame as SparkDataFrame,
)
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StringType, StructField, StructType
from regex import regex
from statsmodels.tsa.arima.model import ARIMA
import numpy as np

from ...data_quality.data_manipulation.interfaces import DataManipulationBaseInterface
from ...data_quality.input_validator import InputValidator
from ...._sdk_utils.pandas import _prepare_pandas_to_convert_to_spark
from ..._pipeline_utils.models import (
    Libraries,
    SystemType,
)


class ArimaPrediction(DataManipulationBaseInterface, InputValidator):
    """
    Extends the timeseries data in given DataFrame with forecasted values from an ARIMA model.
    It forecasts a value column of the given time series dataframe based on the historical data points and constructs
    full entries based on the preceding timestamps. It is advised to place this step after the missing value imputation
    to prevent learning on dirty data.

    It supports dataframes in a source-based format (where each row is an event by a single sensor) and column-based format (where each row is a point in time).

    The similar component AutoArimaPrediction wraps around this component and needs less manual parameters set.

    ARIMA-Specific parameters can be viewed at the following statsmodels documentation page:
    [ARIMA Documentation](https://www.statsmodels.org/dev/generated/statsmodels.tsa.arima.model.ARIMA.html)

    Example
    -------
    ```python
    import numpy as np
    import matplotlib.pyplot as plt
    import numpy.random
    import pandas
    from pyspark.sql import SparkSession

    from rtdip_sdk.pipelines.forecasting.spark.arima import ArimaPrediction

    import rtdip_sdk.pipelines._pipeline_utils.spark as spark_utils

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
    arima_comp = ArimaPrediction(input_df, to_extend_name='Value', number_of_data_points_to_analyze=h_a_l, number_of_data_points_to_predict=h_a_l,
                         order=(3,0,0), seasonal_order=(3,0,0,62))
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
        order (tuple): ARIMA-Specific setting
        seasonal_order (tuple): ARIMA-Specific setting
        trend (str): ARIMA-Specific setting
        enforce_stationarity (bool): ARIMA-Specific setting
        enforce_invertibility (bool): ARIMA-Specific setting
        concentrate_scale (bool): ARIMA-Specific setting
        trend_offset (int): ARIMA-Specific setting
        missing (str): ARIMA-Specific setting
    """

    df: PySparkDataFrame = None
    pd_df: DataFrame = None
    spark_session: SparkSession

    column_to_predict: str
    rows_to_predict: int
    rows_to_analyze: int

    value_name: str
    timestamp_name: str
    source_name: str
    external_regressor_names: List[str]

    class InputStyle(Enum):
        """
        Used to describe style of a dataframe
        """

        COLUMN_BASED = 1  # Schema: [EventTime, FirstSource, SecondSource, ...]
        SOURCE_BASED = 2  # Schema: [EventTime, NameSource, Value, OptionalStatus]

    def __init__(
        self,
        past_data: PySparkDataFrame,
        to_extend_name: str,  # either source or column
        # Metadata about past_date
        past_data_style: InputStyle = None,
        value_name: str = None,
        timestamp_name: str = None,
        source_name: str = None,
        status_name: str = None,
        # Options for ARIMA
        external_regressor_names: List[str] = None,
        number_of_data_points_to_predict: int = 50,
        number_of_data_points_to_analyze: int = None,
        order: tuple = (0, 0, 0),
        seasonal_order: tuple = (0, 0, 0, 0),
        trend=None,
        enforce_stationarity: bool = True,
        enforce_invertibility: bool = True,
        concentrate_scale: bool = False,
        trend_offset: int = 1,
        missing: str = "None",
    ) -> None:
        self.past_data = past_data
        # Convert dataframe to general column-based format for internal processing
        self._initialize_self_df(
            past_data,
            past_data_style,
            source_name,
            status_name,
            timestamp_name,
            to_extend_name,
            value_name,
        )

        if number_of_data_points_to_analyze > self.df.count():
            raise ValueError(
                "Number of data points to analyze exceeds the number of rows present"
            )

        self.spark_session = past_data.sparkSession
        self.column_to_predict = to_extend_name
        self.rows_to_predict = number_of_data_points_to_predict
        self.rows_to_analyze = number_of_data_points_to_analyze or past_data.count()
        self.order = order
        self.seasonal_order = seasonal_order
        self.trend = trend
        self.enforce_stationarity = enforce_stationarity
        self.enforce_invertibility = enforce_invertibility
        self.concentrate_scale = concentrate_scale
        self.trend_offset = trend_offset
        self.missing = missing
        self.external_regressor_names = external_regressor_names

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

    def _initialize_self_df(
        self,
        past_data,
        past_data_style,
        source_name,
        status_name,
        timestamp_name,
        to_extend_name,
        value_name,
    ):
        # Initialize self.df with meta parameters if not already done by previous constructor
        if self.df is None:
            (
                self.past_data_style,
                self.value_name,
                self.timestamp_name,
                self.source_name,
                self.status_name,
            ) = self._constructor_handle_input_metadata(
                past_data,
                past_data_style,
                value_name,
                timestamp_name,
                source_name,
                status_name,
            )

            if self.past_data_style == self.InputStyle.COLUMN_BASED:
                self.df = past_data
            elif self.past_data_style == self.InputStyle.SOURCE_BASED:
                self.df = (
                    past_data.groupby(self.timestamp_name)
                    .pivot(self.source_name)
                    .agg(F.first(self.value_name))
                )
        if not to_extend_name in self.df.columns:
            raise ValueError("{} not found in the DataFrame.".format(to_extend_name))

    def _constructor_handle_input_metadata(
        self,
        past_data: PySparkDataFrame,
        past_data_style: InputStyle,
        value_name: str,
        timestamp_name: str,
        source_name: str,
        status_name: str,
    ) -> Tuple[InputStyle, str, str, str, str]:
        # Infer names of columns from past_data schema. If nothing is found, leave self parameters at None.
        if past_data_style is not None:
            return past_data_style, value_name, timestamp_name, source_name, status_name
        # Automatic calculation part
        schema_names = past_data.schema.names.copy()

        assumed_past_data_style = None
        value_name = None
        timestamp_name = None
        source_name = None
        status_name = None

        def pickout_column(
            rem_columns: List[str], regex_string: str
        ) -> (str, List[str]):
            rgx = regex.compile(regex_string)
            sus_columns = list(filter(rgx.search, rem_columns))
            found_column = sus_columns[0] if len(sus_columns) == 1 else None
            return found_column

        # Is there a status column?
        status_name = pickout_column(schema_names, r"(?i)status")
        # Is there a source name / tag
        source_name = pickout_column(schema_names, r"(?i)tag")
        # Is there a timestamp column?
        timestamp_name = pickout_column(schema_names, r"(?i)time|index")
        # Is there a value column?
        value_name = pickout_column(schema_names, r"(?i)value")

        if source_name is not None:
            assumed_past_data_style = self.InputStyle.SOURCE_BASED
        else:
            assumed_past_data_style = self.InputStyle.COLUMN_BASED

        # if self.past_data_style is None:
        #    raise ValueError(
        #        "Automatic determination of past_data_style failed, must be specified in parameter instead.")
        return (
            assumed_past_data_style,
            value_name,
            timestamp_name,
            source_name,
            status_name,
        )

    def filter_data(self) -> PySparkDataFrame:
        """
        Forecasts a value column of a given time series dataframe based on the historical data points using ARIMA.

        Constructs full entries based on the preceding timestamps. It is advised to place this step after the missing
        value imputation to prevent learning on dirty data.

        Returns:
            DataFrame: A PySpark DataFrame with forecasted value entries depending on constructor parameters.
        """
        # expected_scheme = StructType(
        #    [
        #        StructField("TagName", StringType(), True),
        #        StructField("EventTime", TimestampType(), True),
        #        StructField("Status", StringType(), True),
        #        StructField("Value", NumericType(), True),
        #    ]
        # )
        pd_df = self.df.toPandas()
        pd_df.loc[:, self.timestamp_name] = pd.to_datetime(
            pd_df[self.timestamp_name], format="mixed"
        ).astype("datetime64[ns]")
        pd_df.loc[:, self.column_to_predict] = pd_df.loc[
            :, self.column_to_predict
        ].astype(float)
        pd_df.sort_values(self.timestamp_name, inplace=True)
        pd_df.reset_index(drop=True, inplace=True)
        # self.validate(expected_scheme)

        # limit df to specific data points
        pd_to_train_on = pd_df[pd_df[self.column_to_predict].notna()].tail(
            self.rows_to_analyze
        )
        pd_to_predict_on = pd_df[pd_df[self.column_to_predict].isna()].head(
            self.rows_to_predict
        )
        pd_df = pd.concat([pd_to_train_on, pd_to_predict_on])

        main_signal_df = pd_df[pd_df[self.column_to_predict].notna()]

        input_data = main_signal_df[self.column_to_predict].astype(float)
        exog_data = None
        # if self.external_regressor_names is not None:
        #     exog_data = []
        #     for column_name in self.external_regressor_names:
        #         signal_df = pd.concat([pd_to_train_on[column_name], pd_to_predict_on[column_name]])
        #         exog_data.append(signal_df)

        source_model = ARIMA(
            endog=input_data,
            exog=exog_data,
            order=self.order,
            seasonal_order=self.seasonal_order,
            trend=self.trend,
            enforce_stationarity=self.enforce_stationarity,
            enforce_invertibility=self.enforce_invertibility,
            concentrate_scale=self.concentrate_scale,
            trend_offset=self.trend_offset,
            missing=self.missing,
        ).fit()

        forecast = source_model.forecast(steps=self.rows_to_predict)
        inferred_freq = pd.Timedelta(
            value=statistics.mode(np.diff(main_signal_df[self.timestamp_name].values))
        )

        pd_forecast_df = pd.DataFrame(
            {
                self.timestamp_name: pd.date_range(
                    start=main_signal_df[self.timestamp_name].max() + inferred_freq,
                    periods=self.rows_to_predict,
                    freq=inferred_freq,
                ),
                self.column_to_predict: forecast,
            }
        )

        pd_df = pd.concat([pd_df, pd_forecast_df])

        if self.past_data_style == self.InputStyle.COLUMN_BASED:
            for obj in self.past_data.schema:
                simple_string_type = obj.dataType.simpleString()
                if simple_string_type == "timestamp":
                    continue
                pd_df.loc[:, obj.name] = pd_df.loc[:, obj.name].astype(
                    simple_string_type
                )
            # Workaround needed for PySpark versions <3.4
            pd_df = _prepare_pandas_to_convert_to_spark(pd_df)
            predicted_source_pyspark_dataframe = self.spark_session.createDataFrame(
                pd_df, schema=copy.deepcopy(self.past_data.schema)
            )
            return predicted_source_pyspark_dataframe
        elif self.past_data_style == self.InputStyle.SOURCE_BASED:
            data_to_add = pd_forecast_df[[self.column_to_predict, self.timestamp_name]]
            data_to_add = data_to_add.rename(
                columns={
                    self.timestamp_name: self.timestamp_name,
                    self.column_to_predict: self.value_name,
                }
            )
            data_to_add[self.source_name] = self.column_to_predict
            data_to_add[self.timestamp_name] = data_to_add[
                self.timestamp_name
            ].dt.strftime("%Y-%m-%dT%H:%M:%S.%f")

            pd_df_schema = StructType(
                [
                    StructField(self.source_name, StringType(), True),
                    StructField(self.timestamp_name, StringType(), True),
                    StructField(self.value_name, StringType(), True),
                ]
            )

            # Workaround needed for PySpark versions <3.4
            data_to_add = _prepare_pandas_to_convert_to_spark(data_to_add)

            predicted_source_pyspark_dataframe = self.spark_session.createDataFrame(
                _prepare_pandas_to_convert_to_spark(
                    data_to_add[
                        [self.source_name, self.timestamp_name, self.value_name]
                    ]
                ),
                schema=pd_df_schema,
            )

            if self.status_name is not None:
                predicted_source_pyspark_dataframe = (
                    predicted_source_pyspark_dataframe.withColumn(
                        self.status_name, lit("Predicted")
                    )
                )

            to_return = self.past_data.unionByName(predicted_source_pyspark_dataframe)
            return to_return

    def validate(self, schema_dict, df: SparkDataFrame = None):
        return super().validate(schema_dict, self.past_data)
