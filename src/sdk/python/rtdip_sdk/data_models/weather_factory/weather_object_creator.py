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

from ..constants import weather_constants
from ..weather.AtmosphericG215minForecastV1 import AtmosphericG215minForecastV1


class WeatherObjectCreator:
    @staticmethod
    def create_object(**kwargs):
        version_str: str = kwargs[weather_constants.version]
        if version_str == weather_constants.AtmosphericG215minForecastV1:
            instance_obj = AtmosphericG215minForecastV1()
            return instance_obj.create_object(**kwargs)
        else:
            raise 'Version Not Implemented [{}]'.format(version_str)

