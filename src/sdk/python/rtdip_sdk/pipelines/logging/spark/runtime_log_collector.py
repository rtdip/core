from abc import ABC
from logging import Logger

from pandas import DataFrame

from rtdip_sdk.pipelines._pipeline_utils.models import Libraries, SystemType
from rtdip_sdk.pipelines.logging.interfaces import LoggingBaseInterface
from src.sdk.python.rtdip_sdk.pipelines.logging.logger_manager import LoggerManager
from  src.sdk.python.rtdip_sdk.pipelines.logging.spark.dataframe.dataframe_log_handler import DataFrameLogHandler


class RuntimeLogCollector(LoggingBaseInterface):
    """Collects logs from all loggers in the LoggerManager at runtime."""

    logger_manager: LoggerManager = LoggerManager()
    df_handler: DataFrameLogHandler = DataFrameLogHandler()

    def __init__(self):
      pass

    @classmethod
    def get_logs_as_df(cls) -> DataFrame:
        """Return the DataFrame containing the logs"""
        return cls.df_handler.get_logs_as_df()

    @staticmethod
    def libraries():
        libraries = Libraries()
        return libraries

    @staticmethod
    def settings() -> dict:
        return {}

    @staticmethod
    def system_type() -> SystemType:
        pass

    @classmethod
    def _attach_handler_to_loggers(cls) -> None:
        """Attaches the DataFrameLogHandler to the logger."""

        loggers = cls.logger_manager.get_all_loggers()

        for logger in loggers.values():
            # avoid duplicate handlers
            if cls.df_handler not in logger.handlers:
                print("Attaching handler to logger: ", logger)
                logger.addHandler(cls.df_handler)





