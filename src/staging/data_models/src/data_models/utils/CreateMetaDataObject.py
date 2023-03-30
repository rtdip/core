# Helper class to help create Value Objects and Validate them
from data_models.series.SeriesType import SeriesType
from data_models.metadata.MetaData import MetaData
from data_models.usage.ValueType import ValueType
from data_models.usage.ModelType import ModelType
from data_models.usage.UomUsage import UomUsage
from data_models.logging import logconf

namespace_str: str = 'CreateMetaDataObject'
logger = logconf.get_logger(namespace_str)


def create_metadata_VO(uid: str,
                       series_id: str,
                       series_parent_id: str,
                       name: str,
                       uom: UomUsage,
                       description: str,
                       timestamp_start: int,
                       timestamp_end: int,
                       time_zone: str,
                       version: str,
                       series_type: SeriesType,
                       model_type: ModelType,
                       value_type: ValueType,
                       properties: dict):
    # TODO: check for version
    try:
        return MetaData(uid,
                        series_id,
                        series_parent_id,
                        name,
                        uom,
                        description,
                        timestamp_start,
                        timestamp_end,
                        time_zone,
                        version,
                        series_type,
                        model_type,
                        value_type,
                        properties)
    except Exception as ex:
        error_msg_str: str = 'Could not create Usage Value Object: {}'.format(ex)
        raise SystemError(error_msg_str)
    return metadata_vo
