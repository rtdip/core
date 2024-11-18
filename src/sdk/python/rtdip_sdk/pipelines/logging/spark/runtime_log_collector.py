from abc import ABC
from logging import Logger

from pandas import DataFrame

from rtdip_sdk.pipelines._pipeline_utils.models import Libraries, SystemType
from rtdip_sdk.pipelines.logging.interfaces import LoggingBaseInterface
from rtdip_sdk.pipelines.logging.logger_manager import LoggerManager
from rtdip_sdk.pipelines.logging.spark.dataframe.dataframe_log_handler import DataFrameLogHandler


class RuntimeLogCollector(LoggingBaseInterface):
    """Collects logs from all loggers in the LoggerManager at runtime."""

    logger_manager: LoggerManager = LoggerManager()
    df_handler: DataFrameLogHandler = None

    def __init__(self):
        self.df_handler = DataFrameLogHandler()

    def get_logs_as_df(self) -> DataFrame:
        """Return the DataFrame containing the logs"""
        return self.df_handler.logs_df.copy()

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
        print("ALL Loggers collector: ", loggers)

        for logger in loggers.values():
            # avoid duplicate handlers
            if cls.df_handler not in logger.handlers:
                logger.addHandler(cls.df_handler)





