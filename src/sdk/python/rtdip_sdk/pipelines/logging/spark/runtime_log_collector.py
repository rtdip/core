import os

from pyspark.sql import SparkSession

from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import (
    Libraries,
    SystemType,
)

from src.sdk.python.rtdip_sdk.pipelines.logging.logger_manager import LoggerManager
from src.sdk.python.rtdip_sdk.pipelines.logging.spark.dataframe.dataframe_log_handler import (
    DataFrameLogHandler,
)
from src.sdk.python.rtdip_sdk.pipelines.logging.spark.log_file.file_log_handler import (
    FileLogHandler,
)


class RuntimeLogCollector:
    """Collects logs from all loggers in the LoggerManager at runtime."""

    logger_manager: LoggerManager = LoggerManager()

    spark: SparkSession

    def __init__(self, spark: SparkSession):
        self.spark = spark

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

    def _attach_dataframe_handler_to_logger(
        self, logger_name: str
    ) -> DataFrameLogHandler:
        """Attaches the DataFrameLogHandler to the logger. Returns True if the handler was attached, False otherwise."""
        logger = self.logger_manager.get_logger(logger_name)
        df_log_handler = DataFrameLogHandler(self.spark)
        if logger is not None:
            if df_log_handler not in logger.handlers:
                logger.addHandler(df_log_handler)
        return df_log_handler

    def _attach_file_handler_to_loggers(
        self, filename: str, path: str = ".", mode: str = "a"
    ) -> None:
        """Attaches the FileLogHandler to the logger."""

        loggers = self.logger_manager.get_all_loggers()
        file_path = os.path.join(path, filename)
        file_handler = FileLogHandler(file_path, mode)
        for logger in loggers.values():
            # avoid duplicate handlers
            if file_handler not in logger.handlers:
                logger.addHandler(file_handler)
