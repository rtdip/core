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

from enum import IntFlag, auto


class SeriesType(IntFlag):
    # Keep the order when refactoring.
    real_time = auto()
    minute_1 = auto()
    minutes_5 = auto()
    minutes_10 = auto()
    minutes_15 = auto()
    minutes_30 = auto()
    minutes_60 = auto()
    minutes_2_hours = auto()
    minutes_3_hours = auto()
    minutes_4_hours = auto()
    minutes_6_hours = auto()
    minutes_8_hours = auto()
    minutes_12_hours = auto()
    minutes_24_hours = auto()
    hour = auto()
    day = auto()
    week = auto()
    month = auto()
    year = auto()
    # Computations
    sum = auto()
    average_filter = auto()
    max_filter = auto()
    min_filter = auto()
    # Testing
    test = auto()



