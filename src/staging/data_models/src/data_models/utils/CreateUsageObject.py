from data_models.usage.Usage import Usage
from data_models.logging import logconf

namespace_str: str = 'CreateUsageObject'
logger = logconf.get_logger(namespace_str)


def create_usage_VO(uid: str,
                    series_id: str,
                    timestamp: int,
                    interval_timestamp: int,
                    value: float):
    valid_record_bool: bool = False
    usage_vo: Usage = None
    # TODO: Check for data types and expected values
    try:
        usage_vo: Usage = Usage(uid,
                                series_id,
                                timestamp,
                                interval_timestamp,
                                value)
        valid_record_bool = True

    except Exception as ex:
        error_msg_str: str = 'Could not create Usage Value Object: {}'.format(ex)

    if valid_record_bool:
        return usage_vo
    else:
        # TODO: Send to bad records classifier pipeline
        raise SystemError(error_msg_str)

