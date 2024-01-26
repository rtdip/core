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

from pyspark.sql import SparkSession, DataFrame

from ..._sdk_utils.compare_versions import _package_version_meets_minimum
from ..connection_interface import ConnectionInterface
from ..cursor_interface import CursorInterface
from ...pipelines._pipeline_utils.spark import SparkClient
from ...pipelines._pipeline_utils.models import Libraries
import pandas as pd
import logging


class SparkConnection(ConnectionInterface):
    """
    The Spark Connector enables running Spark Sql queries via a Spark Session.

    Additionally, this connector supports Spark Connect which was introduced in Pyspark 3.4.0 and allows Spark Sessions to connect to remote Spark Clusters. This enables Spark SQL to be constructed locally, but executed remotely.
    To find out more about Spark Connect and the connection string to be provided to the `spark_remote` parameter of the Spark Session, please see [here.](https://spark.apache.org/docs/latest/spark-connect-overview.html#specify-spark-connect-when-creating-spark-session)

    Args:
        spark (optional, SparkSession): Provide an existing spark session if one exists. A new Spark Session will be created if not populated
        spark_configuration (optional, dict): Spark configuration to be provided to the spark session
        spark_libraries (optional, Libraries): Additional JARs to be included in the Spark Session.
        spark_remote (optional, str): Remote connection string of Spark Server and any authentication details. The Spark Connect introduced in Pyspark 3.4.0 allows Spark Sessions to connect to remote Spark Clusters. This enables Spark SQL to be constructed locally, but executed remotely.
    """

    def __init__(
        self,
        spark: SparkSession = None,
        spark_configuration: dict = None,
        spark_libraries: Libraries = None,
        spark_remote: str = None,
    ) -> None:
        if spark_remote != None:
            _package_version_meets_minimum("pyspark", "3.4.0")

        if spark is None:
            self.connection = SparkClient(
                spark_configuration=(
                    {} if spark_configuration is None else spark_configuration
                ),
                spark_libraries=(
                    Libraries() if spark_libraries is None else spark_libraries
                ),
                spark_remote=spark_remote,
            ).spark_session
        else:
            self.connection = spark

    def close(self) -> None:
        """Not relevant for spark sessions"""
        pass

    def cursor(self) -> object:
        """
        Intiates the cursor and returns it.

        Returns:
          DatabricksSQLCursor: Object to represent a databricks workspace with methods to interact with clusters/jobs.
        """
        try:
            return SparkCursor(self.connection)
        except Exception as e:
            logging.exception("error with cursor object")
            raise e


class SparkCursor(CursorInterface):
    """
    Object to represent a spark session with methods to interact with clusters/jobs using the remote connection information.

    Args:
        cursor: controls execution of commands on Spark Cluster
    """

    execute_result: DataFrame

    def __init__(self, cursor: object) -> None:
        self.cursor = cursor

    def execute(self, query: str) -> None:
        """
        Prepares and runs a database query.

        Args:
            query: sql query to execute on the cluster or SQL Warehouse
        """
        try:
            self.execute_result = self.cursor.sql(query)
        except Exception as e:
            logging.exception("error while executing the query")
            raise e

    def fetch_all(self) -> DataFrame:
        """
        Gets all rows of a query.

        Returns:
          DataFrame: Spark DataFrame of results
        """
        try:
            df = self.execute_result
            return df
        except Exception as e:
            logging.exception("error while fetching the rows of a query")
            raise e

    def close(self) -> None:
        """Not relevant for dataframes"""
        pass
