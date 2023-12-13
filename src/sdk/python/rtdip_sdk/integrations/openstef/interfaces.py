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

import geopy
import pandas as pd
import numpy as np
import sqlalchemy
import re
import sqlparams
from datetime import datetime
from sqlalchemy import text

from openstef_dbc.data_interface import _DataInterface
from openstef_dbc import Singleton
from openstef_dbc.ktp_api import KtpApi
from openstef_dbc.log import logging
from ._query_builder import _query_builder

QUERY_ERROR_MESSAGE = "Error occured during executing query"


class _DataInterface(_DataInterface, metaclass=Singleton):
    def __init__(self, config):
        """Generic data interface.

        All connections and queries to the Databricks databases and
        influx API are governed by this class.

        Args:
            config: Configuration object. with the following attributes:
                api_username (str): API username.
                api_password (str): API password.
                api_admin_username (str): API admin username.
                api_admin_password (str): API admin password.
                api_url (str): API url.

                pcdm_host (str): Databricks hostname.
                pcdm_token (str): Databricks token.
                pcdm_port (int): Databricks port.
                pcdm_catalog (str): Databricks catalog.
                pcdm_schema (str): Databricks schema.
                pcdm_http_path (str): SQL warehouse http path.

                db_host (str): Databricks hostname.
                db_token (str): Databricks token.
                db_port (int): Databricks port.
                db_catalog (str): Databricks catalog.
                db_schema (str): Databricks schema.
                db_http_path (str): SQL warehouse http path.
                proxies Union[dict[str, str], None]: Proxies.
        """

        import openstef_dbc.data_interface

        openstef_dbc.data_interface._DataInterface = _DataInterface

        self.logger = logging.get_logger(self.__class__.__name__)

        self.ktp_api = KtpApi(
            username=config.api_username,
            password=config.api_password,
            admin_username=config.api_admin_username,
            admin_password=config.api_admin_password,
            url=config.api_url,
            proxies=config.proxies,
        )

        self.pcdm_engine = self._create_mysql_engine(
            hostname=config.pcdm_host,
            token=config.pcdm_token,
            port=config.pcdm_port,
            http_path=config.pcdm_http_path,
            catalog=config.pcdm_catalog,
            schema=config.pcdm_schema,
        )

        self.mysql_engine = self._create_mysql_engine(
            hostname=config.db_host,
            token=config.db_token,
            port=config.db_port,
            http_path=config.db_http_path,
            catalog=config.db_catalog,
            schema=config.db_schema,
        )

        # Set geopy proxies
        # https://geopy.readthedocs.io/en/stable/#geopy.geocoders.options
        # https://docs.python.org/3/library/urllib.request.html#urllib.request.ProxyHandler
        # By default the system proxies are respected
        # (e.g. HTTP_PROXY and HTTPS_PROXY env vars or platform-specific proxy settings,
        # such as macOS or Windows native preferences – see
        # urllib.request.ProxyHandler for more details).
        # The proxies value for using system proxies is None.
        geopy.geocoders.options.default_proxies = config.proxies
        geopy.geocoders.options.default_user_agent = "rtdip-sdk/0.7.8"

        _DataInterface._instance = self

    @staticmethod
    def get_instance():
        try:
            return Singleton.get_instance(_DataInterface)
        except KeyError as exc:
            # if _DataInterface not in Singleton._instances:
            raise RuntimeError(
                "No _DataInterface instance initialized. "
                "Please call _DataInterface(config) first."
            ) from exc

    def _create_mysql_engine(
        self,
        hostname: str,
        token: str,
        port: int,
        catalog: str,
        schema: str,
        http_path: str,
    ):
        """
        Create Databricks engine.
        """

        conn_string = sqlalchemy.engine.URL.create(
            "databricks",
            username="token",
            password=token,
            host=hostname,
            port=port,
            query={"http_path": http_path, "catalog": catalog, "schema": schema},
        )

        try:
            return sqlalchemy.engine.create_engine(conn_string)
        except Exception as exc:
            self.logger.error("Could not connect to Databricks database", exc_info=exc)
            raise

    def exec_influx_query(self, query: str, bind_params: dict = {}):
        """
        Args:
            query (str): Influx query string.
            bind_params (dict): Binding parameter for parameterized queries

        Returns:
            Pandas Dataframe for single queries or list of Pandas Dataframes for multiple queries.
        """
        try:
            query_list = _query_builder(query)

            if len(query_list) == 1:
                df = pd.read_sql(query_list[0], self.pcdm_engine)
                df["_time"] = pd.to_datetime(df["_time"], utc=True)
                return df
            elif len(query_list) > 1:
                df_list = [pd.read_sql(query, self.pcdm_engine) for query in query_list]
                for df in df_list:
                    df["_time"] = pd.to_datetime(df["_time"], utc=True)
                return df_list

        except Exception as e:
            self.logger.error(QUERY_ERROR_MESSAGE, query=query, exc_info=e)
            raise

    def _check_inputs(self, df: pd.DataFrame, tag_columns: list):
        if type(tag_columns) is not list:
            raise ValueError("'tag_columns' should be a list")

        if len(tag_columns) == 0:
            raise ValueError("At least one tag column should be given in 'tag_columns'")

        # Check if a value is nan
        if True in df.isna().values:
            nan_columns = df.columns[df.isna().any()].tolist()
            raise ValueError(
                f"Dataframe contains NaN's. Found NaN's in columns: {nan_columns}"
            )
        # Check if a value is inf
        if df.isin([np.inf, -np.inf]).any().any():
            inf_columns = df.columns[df.isinf().any()].tolist()
            raise ValueError(
                f"Dataframe contains Inf's. Found Inf's in columns: {inf_columns}"
            )

        if True in df.isnull().values:
            nan_columns = df.columns[df.isnull().any()].tolist()
            raise ValueError(
                f"Dataframe contains missing values. Found missing values in columns: {nan_columns}"
            )

        if set(tag_columns).issubset(set(list(df.columns))) is False:
            tag_cols = [x for x in tag_columns if x not in list(df.columns)]
            raise ValueError(
                f"Dataframe missing tag columns. Missing tag columns: {tag_cols}"
            )

    def exec_influx_write(
        self,
        df: pd.DataFrame,
        database: str,
        measurement: str,
        tag_columns: list,
        organization: str = None,
        field_columns: list = None,
        time_precision: str = "s",
    ) -> bool:
        self._check_inputs(df, tag_columns)

        if field_columns is None:
            field_columns = [x for x in list(df.columns) if x not in tag_columns]

        tag_columns = sorted(tag_columns)

        id_vars = ["EventTime"] + tag_columns

        casting_tags = {}
        casting_tags.update(dict.fromkeys(tag_columns, str))

        df = df.astype(casting_tags)

        casting_fields = {
            "algtype": "str",
            "clearSky_dlf": "float",
            "clearSky_ulf": "float",
            "clouds": "float",
            "clouds_ensemble": "float",
            "created": "int",
            "customer": "str",
            "description": "str",
            "ensemble_run": "str",
            "forecast": "float",
            "forecast_other": "float",
            "forecast_solar": "float",
            "forecast_wind_on_shore": "float",
            "grnd_level": "float",
            "humidity": "float",
            "input_city": "str",
            "mxlD": "float",
            "output": "float",
            "pid": "int",
            "prediction": "float",
            "pressure": "float",
            "quality": "str",
            "radiation": "float",
            "radiation_direct": "float",
            "radiation_diffuse": "float",
            "radiation_ensemble": "float",
            "radiation_normal": "float",
            "rain": "float",
            "sea_level": "float",
            "snowDepth": "float",
            "source": "str",
            "source_run": "int",
            "stdev": "float",
            "system": "str",
            "tAhead": "float",
            "temp": "float",
            "temp_kf": "float",
            "temp_min": "float",
            "temp_max": "float",
            "type": "str",
            "winddeg": "float",
            "winddeg_ensemble": "float",
            "window_days": "float",
            "windspeed": "float",
            "windspeed_100m": "float",
            "windspeed_100m_ensemble": "float",
            "windspeed_ensemble": "float",
        }

        p = re.compile(r"quantile_")
        quantile_columns = [s for s in field_columns if p.match(s)]
        casting_fields.update(dict.fromkeys(quantile_columns, "float"))

        if measurement == "prediction_kpi":
            intcols = ["pid"]
            floatcols = [x for x in df.columns if x not in intcols]
            casting_fields.update(dict.fromkeys(floatcols, "float"))

        if measurement == "sjv" or measurement == "marketprices":
            casting_fields.update(dict.fromkeys(list(df.columns)[:-1], "float"))

        df.index = df.index.strftime("%Y-%m-%dT%H:%M:%S")
        df = df.reset_index(names=["EventTime"])
        df = pd.melt(
            df,
            id_vars=id_vars,
            value_vars=field_columns,
            var_name="_field",
            value_name="Value",
        )

        if measurement == "weather":
            list_of_cities = df["input_city"].unique()
            coordinates = {}

            for city in list_of_cities:
                location = geopy.geocoders.Nominatim().geocode(city)
                location = (location.latitude, location.longitude)
                coordinates.update({city: location})

            df["Latitude"] = df["input_city"].map(lambda x: coordinates[x][0])
            df["Longitude"] = df["input_city"].map(lambda x: coordinates[x][1])
            df["EnqueuedTime"] = datetime.now()
            df["Latest"] = True
            df["EventDate"] = df["EventTime"].dt.date
            df["TagName"] = df[["_field"] + tag_columns].apply(":".join, axis=1)
            tag_columns.remove("source")
            df.rename(columns={"source": "Source"})
        else:
            df["TagName"] = df[["_field"] + tag_columns].apply(":".join, axis=1)

        df["Status"] = "Good"
        df.drop(columns=tag_columns + ["_field"], inplace=True)

        # Write to different tables
        df_cast = df.copy()
        df_cast["ValueType"] = df_cast["TagName"].str.split(":").str[0]
        df_cast["ValueType"] = df_cast["ValueType"].map(casting_fields)

        int_df = df_cast.loc[df_cast["ValueType"] == "int"].copy()
        int_df.drop(columns=["ValueType"], inplace=True)
        int_df = int_df.astype({"Value": np.int64})

        float_df = df_cast.loc[df_cast["ValueType"] == "float"].copy()
        float_df.drop(columns=["ValueType"], inplace=True)
        float_df = float_df.astype({"Value": np.float64})

        str_df = df_cast.loc[df_cast["ValueType"] == "str"].copy()
        str_df.drop(columns=["ValueType"], inplace=True)
        str_df = str_df.astype({"Value": str})

        df = df.astype({"Value": str})

        dataframes = [
            (df, measurement),
            (int_df, measurement + "_restricted_events_integer"),
            (float_df, measurement + "_restricted_events_float"),
            (str_df, measurement + "_restricted_events_string"),
        ]

        try:
            for df, measurement in dataframes:
                if not df.empty:
                    df.to_sql(
                        measurement,
                        self.pcdm_engine,
                        if_exists="append",
                        index=False,
                        method="multi",
                    )
            return True
        except Exception as e:
            self.logger.error(
                "Exception occured during writing to Databricks database", exc_info=e
            )
            raise

    def check_influx_available(self):
        return self.check_mysql_available()

    def exec_sql_query(self, query: str, params: dict = None, **kwargs):
        if params is None:
            params = {}

        if " join " in query.lower().replace(
            "\t", " "
        ) and " on " not in query.lower().replace("\t", " "):
            join_pattern = re.compile(r"JOIN\s+\((.*?)\)", re.IGNORECASE | re.DOTALL)
            matches = re.search(join_pattern, query).group(1)
            joins = [f"CROSS JOIN {x.strip()}" for x in matches.split(",")]
            query = re.sub(join_pattern, " ".join(joins), query)

        pattern = re.compile(r"GROUP BY \w+\.\w+", re.IGNORECASE | re.DOTALL)
        query = pattern.sub("GROUP BY ALL", query).replace(
            "HAVING", "GROUP BY ALL HAVING"
        )

        new_query = []
        words = query.split()
        for i in range(0, len(words)):
            if "%" in words[i] and words[i - 1].lower() == "like":
                new_query.append("%(" + words[i].replace("'", "") + ")s")
                params[words[i].replace("'", "")] = words[i].replace("'", "")
            else:
                new_query.append(words[i])

        query = " ".join(new_query)

        try:
            return pd.read_sql(query, self.mysql_engine, params=params, **kwargs)
        except sqlalchemy.exc.OperationalError as e:
            self.logger.error("Lost connection to Databricks database", exc_info=e)
            raise
        except sqlalchemy.exc.ProgrammingError as e:
            self.logger.error(QUERY_ERROR_MESSAGE, query=query, exc_info=e)
            raise
        except sqlalchemy.exc.DatabaseError as e:
            self.logger.error("Can't connect to Databricks database", exc_info=e)
            raise

    def exec_sql_write(self, statement: str, params: dict = None) -> None:
        if params is None:
            params = {}

        for key in params.keys():
            if "table" in key.lower():
                statement = statement.replace(f"%({key})s", params[f"{key}"])

        if re.search(re.compile(r"INSERT\sIGNORE", re.IGNORECASE), statement):
            values = re.search(
                re.compile(r"VALUES\s(.*?)\)", re.IGNORECASE), statement
            ).group(0)
            table = re.search(
                re.compile(r"INTO\s(.*?)\s\(", re.IGNORECASE), statement
            ).group(1)
            columns = re.search(r"\((.*?)\)", statement).group(0)
            columns_list = re.search(r"\((.*?)\)", statement).group(1).split(",")

            source_cols = ""
            for i in range(len(columns_list)):
                source_cols += f"source.col{i+1}, "

            source_cols = source_cols[:-2]

            statement = f"""
            MERGE INTO {table}
            USING ({values}) AS source
                    ON {table}.{columns_list[0].strip()} = source.col1
                    WHEN NOT MATCHED THEN
            INSERT {columns}
                    VALUES ({source_cols});
                    """

        query_format = sqlparams.SQLParams("pyformat", "named")
        statement, params = query_format.format(statement, params)

        try:
            with self.mysql_engine.connect() as connection:
                response = connection.execute(statement, params=params)

                self.logger.info(
                    "Added {} new systems to the systems table in the MySQL database".format(
                        response.rowcount
                    )
                )

        except Exception as e:
            self.logger.error(QUERY_ERROR_MESSAGE, query=statement, exc_info=e)
            raise

    def exec_sql_dataframe_write(
        self, dataframe: pd.DataFrame, table: str, **kwargs
    ) -> None:
        dataframe.to_sql(table, self.mysql_engine, **kwargs)

    def check_mysql_available(self):
        """Check if a basic Databricks SQL query gives a valid response"""
        query = "SHOW DATABASES"
        response = self.exec_sql_query(query)

        available = len(list(response["Database"])) > 0

        return available
