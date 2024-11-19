import os

from pandas import DataFrame
from pandas.io.common import file_path_to_url

from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import (
    Libraries,
    SystemType,
)
from src.sdk.python.rtdip_sdk.pipelines.logging.interfaces import LoggingBaseInterface
from src.sdk.python.rtdip_sdk.pipelines.logging.logger_manager import LoggerManager
from src.sdk.python.rtdip_sdk.pipelines.logging.spark.dataframe.dataframe_log_handler import (
    DataFrameLogHandler,
)
from src.sdk.python.rtdip_sdk.pipelines.logging.spark.log_file.file_log_handler import (
    FileLogHandler,
)


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
    def _attach_dataframe_handler_to_loggers(cls) -> None:
        """Attaches the DataFrameLogHandler to the logger."""

        loggers = cls.logger_manager.get_all_loggers()

        for logger in loggers.values():
            # avoid duplicate handlers
            if cls.df_handler not in logger.handlers:
                logger.addHandler(cls.df_handler)

    @classmethod
    def _attach_file_handler_to_loggers(
        cls, filename: str, path: str = ".", mode: str = "a"
    ) -> None:
        """Attaches the FileLogHandler to the logger."""

        loggers = cls.logger_manager.get_all_loggers()
        file_path = os.path.join(path, filename)
        file_handler = FileLogHandler(file_path, mode)
        for logger in loggers.values():
            # avoid duplicate handlers
            if file_handler not in logger.handlers:
                logger.addHandler(file_handler)
