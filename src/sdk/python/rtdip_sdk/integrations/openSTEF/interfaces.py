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

import re
import geopy
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
from jinja2 import Template
import pandas as pd
import numpy as np
import requests
import sqlalchemy

from openstef_dbc.data_interface import _DataInterface
from openstef_dbc import Singleton
from openstef_dbc.ktp_api import KtpApi
from openstef_dbc.log import logging
from ._query_builder import _query_builder

class _DataInterface(_DataInterface, metaclass=Singleton):
    def __init__(self, config):
        """Generic data interface.

        All connections and queries to the InfluxDB database, MySQL databases and
        influx API are governed by this class.

        Args:
            config: Configuration object. with the following attributes:
                api_username (str): API username.
                api_password (str): API password.
                api_admin_username (str): API admin username.
                api_admin_password (str): API admin password.
                api_url (str): API url.
                db_host (str): Databricks hostname.
                db_token (str): Databricks token.
                db_port (int): Databricks port.
                db_catalog (str): Databricks catalog.
                db_schema (str): Databricks schema.
                db_http_path (str): SQL warehouse http path.
                proxies Union[dict[str, str], None]: Proxies.
        """

        self.logger = logging.get_logger(self.__class__.__name__)

        self.ktp_api = KtpApi(
            username=config.api_username,
            password=config.api_password,
            admin_username=config.api_admin_username,
            admin_password=config.api_admin_password,
            url=config.api_url,
            proxies=config.proxies,
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
        geopy.geocoders.options.default_user_agent = "rtdip/0.7.8" # User-Agent header to send with the requests to geocoder API -> User-Agent: <product> / <product-version> <comment>

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
        """Create MySQL engine.

        Differs from sql_connection in the sense that this write_engine
        *can* write pandas dataframe directly.

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
            self.logger.error("Could not connect to MySQL database", exc_info=exc)
            raise

    def exec_influx_query(self, query: str, bind_params: dict = {}):
        """Execute an InfluxDB query.

        When there is data it returns a defaultdict with as key the measurement and
        as value a DataFrame. When there is NO data it returns an empty dictionairy.

        Args:
            query (str): Influx query string.
            bind_params (dict): Binding parameter for parameterized queries

        Returns:
            defaultdict: Query result.
        """
        try:
            query_list = _query_builder(query)

            if len(query_list) == 1:
                return pd.read_sql(query_list[0], self.mysql_engine)
            elif len(query_list) > 1:
                df = [pd.read_sql(query, self.mysql_engine) for query in query_list]
                return df

        except Exception as e:
            self.logger.error(
                "Error occured during executing InfluxDB query", query=query, exc_info=e
            )
            raise

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
        return None

    def check_influx_available(self):
        return None

    def exec_sql_query(self, query: str, params: dict = None, **kwargs):
        if params is None:
            params = {}
        try:
            return pd.read_sql(query, self.mysql_engine, params=params, **kwargs)
        except sqlalchemy.exc.OperationalError as e:
            self.logger.error("Lost connection to MySQL database", exc_info=e)
            raise
        except sqlalchemy.exc.ProgrammingError as e:
            self.logger.error(
                "Error occured during executing query", query=query, exc_info=e
            )
            raise
        except sqlalchemy.exc.DatabaseError as e:
            self.logger.error("Can't connect to MySQL database", exc_info=e)
            raise

    def exec_sql_write(self, statement: str, params: dict = None) -> None:
        if params is None:
            params = {}
        try:
            with self.mysql_engine.connect() as connection:
                connection.execute(statement, params=params)
        except Exception as e:
            self.logger.error(
                "Error occured during executing query", query=statement, exc_info=e
            )
            raise

    def exec_sql_dataframe_write(
        self, dataframe: pd.DataFrame, table: str, **kwargs
    ) -> None:
        dataframe.to_sql(table, self.mysql_engine, index=False, **kwargs)

    def check_mysql_available(self):
        """Check if a basic mysql query gives a valid response"""
        query = "SHOW DATABASES"
        response = self.exec_sql_query(query)

        available = len(list(response["Database"])) > 0

        return available
