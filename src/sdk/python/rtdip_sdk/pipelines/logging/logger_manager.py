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


from pyspark.pandas.usage_logging.usage_logger import get_logger


class LoggerManager:
    """
    Manages creation and storage of all loggers in the application. This is a singleton class.
    Please create loggers with the LoggerManager if you want your logs to be handled and stored properly.


    Example Usage
    --------
    ```python
    logger_manager = LoggerManager()
    logger = logger_manager.create_logger("my_logger")
    logger.info("This is a log message")
    my_logger = logger_manager.get_logger("my_logger")
    ```
    """

    _instance = None
    _initialized = False

    # dictionary to store all loggers
    loggers = {}

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(LoggerManager, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if not LoggerManager._initialized:
            logging.basicConfig(
                level=logging.INFO,
                format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            )
            LoggerManager._initialized = True

    @classmethod
    def create_logger(cls, name: str):
        """
        Creates a logger with the specified name.

        Args:
            name (str): The name of the logger.

        Returns:
            logging.Logger: Configured logger instance.
        """
        if name not in cls.loggers:
            logger = logging.getLogger(name)
            cls.loggers[name] = logger
            return logger

        return cls.get_logger(name)

    @classmethod
    def get_logger(cls, name: str):
        if name not in cls.loggers:
            return None
        return cls.loggers[name]

    @classmethod
    def get_all_loggers(cls) -> dict:
        return cls.loggers
