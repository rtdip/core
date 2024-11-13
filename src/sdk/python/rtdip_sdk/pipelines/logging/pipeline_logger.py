import logging

class PipelineLogger:
    _logger = None

    @classmethod
    def get_logger(cls, name: str = "PipelineLogger") -> logging.Logger:
        if cls._logger is None:
            cls._logger = logging.getLogger(name)
            cls._logger.setLevel(logging.DEBUG)
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            cls._logger.addHandler(handler)
        return cls._logger