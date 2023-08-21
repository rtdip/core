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

from pandas import DataFrame
from .compare_versions import _package_version_meets_minimum


def _prepare_pandas_to_convert_to_spark(df: DataFrame) -> DataFrame:
    # Spark < 3.4.0 does not support iteritems method in Pandas > 2.0.1
    try:
        _package_version_meets_minimum("pandas", "2.0.0")
        try:
            _package_version_meets_minimum("pyspark", "3.4.0")
        except:
            df.iteritems = df.items
        return df
    except:
        return df
