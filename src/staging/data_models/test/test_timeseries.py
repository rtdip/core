#!/usr/bin/env python
from data_models.series.SeriesType import SeriesType
from data_models.utils import CreateMetaDataObject
from data_models.metadata.MetaData import MetaData
from data_models.usage.ValueType import ValueType
from data_models.usage.ModelType import ModelType
from data_models.utils import CreateUsageObject
from data_models.usage.UomUsage import UomUsage
from data_models.usage.Version import Version
from data_models.usage.Usage import Usage
from data_models.logging import logconf
from data_models.utils import utils
from uuid import uuid4
import unittest
import tzlocal  # External Dependency

namespace_str: str = 'test_timeseries'
logger = logconf.get_logger(namespace_str)


class TestGenerateTimeseries(unittest.TestCase):

    def setUp(self):
        pass

    def test_generate_timeseries_objects_creation_ser_deser(self):

        #
        meter_1_uid_str: str = str(uuid4())
        series_1_id_str: str = str(uuid4())
        series_1_parent_id_str: str = None
        description_str: str = 'description_' + str(uuid4())

        version_str: str = Version.Version_0_0_1 + '_' + str(uuid4())

        timestamp_start_int: int = int(utils.get_utc_timestamp())
        timestamp_end_int: int = timestamp_start_int
        timezone_str: str = tzlocal.get_localzone_name()
        name_str: str = 'name_' + str(uuid4())
        uom = UomUsage.kw

        series_type = SeriesType.minutes_10
        model_type = ModelType.default
        value_type = ValueType.usage

        properties_dict: dict = dict()
        key_str: str = 'key_' + str(uuid4())
        value_str: str = 'value_' + str(uuid4())
        properties_dict[key_str] = value_str

        series_1_metadata: MetaData = CreateMetaDataObject.create_metadata_VO(meter_1_uid_str,
                                                                              series_1_id_str,
                                                                              series_1_parent_id_str,
                                                                              name_str,
                                                                              uom,
                                                                              description_str,
                                                                              timestamp_start_int,
                                                                              timestamp_end_int,
                                                                              timezone_str,
                                                                              version_str,
                                                                              series_type,
                                                                              model_type,
                                                                              value_type,
                                                                              properties_dict)

        # Test for json ser./deser. Metadata
        series_1_metadata_json = series_1_metadata.to_json()
        series_1_metadata_loaded: MetaData = MetaData.from_json(series_1_metadata_json)
        self.assertTrue(series_1_metadata_loaded.properties[key_str] == value_str)

        # Test for json serialization/deser. Usage
        timestamp_interval_int: int = timestamp_start_int
        usage_vo: Usage = CreateUsageObject.create_usage_VO(meter_1_uid_str,
                                                            series_1_id_str,
                                                            timestamp_start_int,
                                                            timestamp_interval_int,
                                                            utils.generate_random_int_number(0,1000) * 0.1)
        logger.debug(usage_vo)
        self.assertTrue(Usage.from_json(usage_vo.to_json()).uid == meter_1_uid_str)

    def tearDown(self):
        pass


def tests():
    test_suite = unittest.TestSuite()
    test_suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestGenerateTimeseries))
    return test_suite


if __name__ == "__main__":
    unittest.TextTestRunner(verbosity=2).run(tests())
