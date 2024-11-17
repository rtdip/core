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
    Singleton that manages the creation of loggers. Stores all loggers in a dictionary.
    """

    __instance = None

    # dictionary to store all loggers
    loggers = {}

    @staticmethod
    def get_instance():
        if LoggerManager.__instance is None:
            return LoggerManager()
        return LoggerManager.__instance


    def __init__(self):
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    @classmethod
    def create_logger(cls, name: str):
        """
        Creates a logger with the specified name.

        Args:
            name (str): The name of the logger.

        Returns:
            logging.Logger: Configured logger instance.
        """
        if name not in cls.get_instance().loggers:
            logger = logging.getLogger(name)
            cls.get_instance().loggers[name] = logger
            return logger

        return get_logger(cls, name)

    @classmethod
    def get_logger(cls, name:str):
        if name not in cls.get_instance().loggers:
            return None
        return cls.get_instance().loggers[name]

