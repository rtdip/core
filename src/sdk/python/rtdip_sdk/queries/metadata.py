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

import logging
<<<<<<<< HEAD:src/sdk/python/rtdip_sdk/queries/metadata.py
import pandas as pd
from .time_series._query_builder import _query_builder
========
logging.warning('Module rtdip_sdk.functions is deprecated and will be removed in v1.0.0. Please import rtdip_sdk.queries instead.')
>>>>>>>> develop:src/sdk/python/rtdip_sdk/functions/metadata.py

from ..queries.metadata import * # NOSONAR