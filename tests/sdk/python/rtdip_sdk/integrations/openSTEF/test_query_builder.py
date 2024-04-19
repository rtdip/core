# Copyright 2023 RTDIP
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys

sys.path.insert(0, ".")
import pytest
from src.sdk.python.rtdip_sdk.integrations.openstef.interfaces import _query_builder
from .interface_test_objects import (
    RAW_WEATHER_QUERY,
    RAW_PREDICTION_QUERY,
    RESAMPLE_CREATE_EMPTY_FALSE,
    RESAMPLE_CREATE_EMPTY_TRUE,
    MAX_QUERY,
    PIVOT_QUERY,
    MULTI_RESAMPLE_QUERY,
    RAW_WEATHER_SQL_QUERY,
    RAW_PREDICTION_SQL_QUERY,
    RESAMPLE_CREATE_EMPTY_FALSE_SQL,
    RESAMPLE_CREATE_EMPTY_TRUE_SQL,
    MAX_SQL_QUERY,
    PIVOT_SQL_QUERY,
    MULTI_RESAMPLE_SQL_QUERY,
)


@pytest.mark.parametrize(
    "query, result",
    [
        (RAW_WEATHER_QUERY, RAW_WEATHER_SQL_QUERY),
        (RAW_PREDICTION_QUERY, RAW_PREDICTION_SQL_QUERY),
        (RESAMPLE_CREATE_EMPTY_FALSE, RESAMPLE_CREATE_EMPTY_FALSE_SQL),
        (RESAMPLE_CREATE_EMPTY_TRUE, RESAMPLE_CREATE_EMPTY_TRUE_SQL),
        (MAX_QUERY, MAX_SQL_QUERY),
        (PIVOT_QUERY, PIVOT_SQL_QUERY),
        (MULTI_RESAMPLE_QUERY, MULTI_RESAMPLE_SQL_QUERY),
    ],
)
def test_query_builder(query, result):
    query_result = _query_builder(query)
    assert isinstance(query_result, list)
    assert query_result == result
