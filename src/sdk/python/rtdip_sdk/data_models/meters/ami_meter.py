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


from pydantic.v1 import BaseModel


class Usage(BaseModel):
    """
    Usage. a usage measurement from an AMI meter
    """

    Uid: str
    """
    A unique identifier associated to the source of the measurement (e.g. sensor, meter, etc.)
    """
    SeriesId: str
    """
    Identifier for a particular timeseries set
    """
    Timestamp: int
    """
    Creation time. Always UTC. Seconds since EPOCH
    """
    IntervalTimestamp: int
    """
    The timestamp for the interval. Always UTC. Seconds since EPOCH
    """
    Value: float
    """
    The actual value of the measurement
    """
