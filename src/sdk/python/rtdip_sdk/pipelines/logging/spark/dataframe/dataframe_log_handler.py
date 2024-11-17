# Copyright 2024 RTDIP
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
from abc import abstractmethod, ABC

from pandas import DataFrame
from datetime import datetime

class LoggingCollectorDataframe:

    """
    Collect logs from loggers contained in LoggerManager and stores them in a DataFrame at runtime
    """
    logs_df: DataFrame = None
    def __init__(self):
        self.logs_df = DataFrame(columns = ['timestamp', 'name', 'level', 'message'])

    def emit(self, record: logging.LogRecord) -> None:
        """Process and store a log record"""
        log_entry = {
            'timestamp': datetime.fromtimestamp(record.created),
            'name': record.name,
            'level': record.levelname,
            'message': record.message
        }

        self.logs_df = self.logs_df.append(log_entry, ignore_index=True)













