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


from pandas import DataFrame
from datetime import datetime


class FileLogHandler(logging.Handler):
    """
    Handles logs from attached logger and stores them in a .log file

    Args:
        logging.Handler: Inherits from logging.Handler
        filename (str): Name of the log file to write to
        mode (str): File opening mode ('a' for append, 'w' for write)

    Example
    --------
    ```python
    import logging

    log_manager = logging.getLogger('log_manager')
    handler = FileLogHandler('my_logs.log')
    log_manager.addHandler(handler)
    ```
    """

    logs_df: DataFrame = None

    def __init__(self, file_path: str, mode: str = "a"):
        super().__init__()
        self.mode = mode
        self.file_path = file_path

    def emit(self, record: logging.LogRecord) -> None:
        """Process and store a log record in the log file"""
        try:
            log_entry = {
                f"{datetime.fromtimestamp(record.created).isoformat()} | "
                f"{record.name} | "
                f"{record.levelname} | "
                f"{record.msg}\n"
            }
            with open(self.file_path, self.mode, encoding="utf-8") as log_file:
                log_file.write(str(log_entry) + "\n")

        except Exception as e:
            print(f"Error writing log entry to file: {e}")
