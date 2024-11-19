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

import pandas
from pandas import DataFrame
from datetime import datetime



class DataFrameLogHandler(logging.Handler):

    """
    Handles logs from attached logger and stores them in a DataFrame at runtime

     Args:
        logging.Handler: Inherits from logging.Handler

    Returns:
        returns a DataFrame with logs stored in it

    Example
    --------
    ```python
    import logging

    log_manager = logging.getLogger('log_manager')

    """
    logs_df: DataFrame = None

    def __init__(self):
        self.logs_df = DataFrame(columns = ['timestamp', 'name', 'level', 'message'])
        super().__init__()

    def emit(self, record: logging.LogRecord) -> None:
        """Process and store a log record"""
        log_entry = {
            'timestamp': datetime.fromtimestamp(record.created),
            'name': record.name,
            'level': record.levelname,
            'message': record.msg
        }

        new_log_df_row = pandas.DataFrame(log_entry, columns = ['timestamp', 'name', 'level', 'message'], index=[0])
        self.logs_df = pandas.concat([self.logs_df, new_log_df_row], ignore_index=True)

    def get_logs_as_df(self) -> DataFrame:
        return self.logs_df



