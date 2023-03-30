#!/usr/bin/env python
from data_models.usage.ValueType import ValueType
from data_models.logging import logconf
import unittest


namespace_str: str = 'test_model_type'
logger = logconf.get_logger(namespace_str)


class TestTimeModelType(unittest.TestCase):

    def setUp(self):
        pass

    def test_value_type(self):
        value_type:  ValueType = ValueType(0)
        logger.debug(value_type)
        logger.debug(value_type.backcast)
        logger.debug(value_type.backcast.value)
        logger.debug(value_type.forecast)
        logger.debug(value_type.forecast.value)
        self.assertTrue(value_type.long_term_term_forecast.value == value_type.forecast | value_type.long_term)

    def tearDown(self):
        pass


def tests():
    test_suite = unittest.TestSuite()
    test_suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestTimeModelType))
    return test_suite


if __name__ == "__main__":
    unittest.TextTestRunner(verbosity=2).run(tests())
