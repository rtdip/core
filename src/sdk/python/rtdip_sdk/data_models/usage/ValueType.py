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


class ValueType(IntFlag):
    # Keep the order when refactoring.
    counter = auto()
    gauge = auto()
    histogram = auto()
    summary = auto()
    usage = auto()
    generation = auto()
    prediction = auto()
    short_term = auto()
    long_term = auto()
    backcast = auto()
    forecast = auto()
    short_term_backcast = short_term | backcast
    long_term_term_backcast = long_term | backcast
    short_term_forecast = short_term | forecast
    long_term_term_forecast = long_term | forecast
    # ...






