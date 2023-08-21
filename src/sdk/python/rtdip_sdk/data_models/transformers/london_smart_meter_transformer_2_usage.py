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

from ..meters.utils import transformers

import datetime
import hashlib
import time


series_id_str = "usage_series_id_001"
output_header_str: str = "Uid,SeriesId,Timestamp,IntervalTimestamp,Value"

transformer_method_str: str = transformers.LAMBDA_TRANSFORM_METHOD_CHECK


def anonymizer_md5(input_str: str) -> str:
    """
    Generates the md5 hash of the input
    a
    """
    result = hashlib.md5(input_str.encode())  # NOSONAR
    return str(result.hexdigest())


transformer_configuration = (
    lambda input_list: str(anonymizer_md5(input_list[0]))
    + ","
    + series_id_str
    + "_"
    + input_list[1]
    + ","
    + str(
        int(
            time.mktime(
                datetime.datetime.strptime(
                    str(input_list[2]).replace(".0", "."), "%Y-%m-%d %H:%M:%S.%f"
                ).timetuple()
            )
        )
    )
    + ","
    + str(
        int(
            time.mktime(
                datetime.datetime.strptime(
                    str(input_list[2]).replace(".0", "."), "%Y-%m-%d %H:%M:%S.%f"
                ).timetuple()
            )
        )
    )
    + ","
    + str(float(input_list[3])).replace(" ", "")
    + "\n"
)
