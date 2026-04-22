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
import importlib.util

from .odbc.db_sql_connector import *

if importlib.util.find_spec("pyodbc") != None:
    from .odbc.pyodbc_sql_connector import *
if importlib.util.find_spec("turbodbc") != None:
    import warnings

    warnings.warn(
        "TURBODBC connector is deprecated and no longer inherently supported in RTDIP (as of v0.14.4). "
        "You have two options: (1) Use an older RTDIP version prior to v0.14.4, or (2) Manually install turbodbc. "
        "Consider using Databricks SQL Connector or PYODBC SQL Connector instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    from .odbc.turbodbc_sql_connector import *
if importlib.util.find_spec("pyspark") != None:
    from .grpc.spark_connector import *
from .models import *
