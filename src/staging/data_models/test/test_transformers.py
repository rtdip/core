#!/usr/bin/env python


from data_models.utils import transformers
from data_models.logging import logconf
from data_models.utils import utils
from datetime import date
import datetime
import unittest
import logging


namespace_str: str = 'test_transformers'
logger = logconf.get_logger(namespace_str)


class TestTransformers(unittest.TestCase):
    logger.setLevel(logging.DEBUG)

    def setUp(self):
        pass

    def test_LAMBDA_TRANSFORM_TYPE_CHECK(self):
        # Transformation. Type Check
        # 0 if value matches expected type
        transformer_option_expected_types = \
            lambda input_list: [0 if utils.infer_type(input_list[0]) is str else 1,
                                0 if utils.infer_type(input_list[1]) is int else 1,
                                0 if utils.infer_type(input_list[2]) is float else 1,
                                0 if utils.infer_type(input_list[3]) is date else 1,
                                0 if utils.infer_type(input_list[4]) is datetime.datetime else 1]

        ###

        ###

        dt_str: str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")



        # String, Integer, Float, Date, DateTime. Sample record
        test_input_record_sample_date_str: str = 'this is a string,' + \
                                                 str(utils.generate_random_int_number(10, 100)) + ',' \
                                                 + str(utils.generate_random_int_number(10, 100) * 0.1) + ',' \
                                                 + str(date.today()) + ',' \
                                                 + dt_str
        logger.debug('test_input_record: {}'.format(test_input_record_sample_date_str))
        try:

            transformer_options_list: list = list()
            transformer_options_list.append(transformer_option_expected_types)

            # Transformers will compute if record has the expected types
            result_list: list = getattr(transformers, transformers.LAMBDA_TRANSFORM_METHOD_CHECK)(
                test_input_record_sample_date_str, transformer_options_list)
            logger.debug('Source Record     : [{}]'.format(test_input_record_sample_date_str))
            logger.debug('Transformed Record: {}'.format(result_list))

            # Compute if all columns match ( 0 = all match)
            if sum(i for i in result_list) == 0:
                logger.debug('Record has the right types')
            else:
                self.fail()
        except Exception as ex:
            logger.error(ex)
            self.fail()

    def test_LAMBDA_TRANSFORM_TYPES_AND_RANGE_CHECK(self):
        # Transformation. Type Check
        # 0 if value matches expected type
        transformer_configuration = \
            lambda input_list: [0 if utils.infer_type(input_list[0]) is str else 1,
                                0 if (0 < int(input_list[1]) < 10) else 1,
                                0 if utils.infer_type(input_list[2]) is float else 1,
                                0 if utils.infer_type(input_list[3]) is date else 1,
                                0 if utils.infer_type(input_list[4]) is datetime.datetime else 1]

        dt_str: str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        # String, Integer, Float, Date, DateTime. Sample record
        expected_types_list: list = [str, int, float, date, datetime]
        logger.debug(expected_types_list)
        test_input_record_sample_date_str: str = 'this is a string,' + \
                                                 "9" + ',' \
                                                 + str(utils.generate_random_int_number(10, 100) * 0.1) + ',' \
                                                 + str(date.today()) + ',' \
                                                 + dt_str
        logger.debug('test_input_record: {}'.format(test_input_record_sample_date_str))
        try:

            transformer_options_list: list = list()
            transformer_options_list.append(transformer_configuration)

            # Transformers will compute if record has the expected types
            result_list: list = getattr(transformers, transformers.LAMBDA_TRANSFORM_METHOD_CHECK)(
                test_input_record_sample_date_str, transformer_options_list)
            logger.debug('Expected source types: {}'.format(expected_types_list))
            logger.debug('Source Record     : [{}]'.format(test_input_record_sample_date_str))
            logger.debug('Transformed Record: {}'.format(result_list))

            # Compute if all columns match ( 0 = all match)
            if sum(i for i in result_list) == 0:
                logger.debug('Record has the right types')
            else:
                self.fail()
        except Exception as ex:
            logger.error(ex)
            self.fail()

    def test_LAMBDA_TRANSFORM_TYPE_CHECK_one_wrong_type(self):
        # Transformation. Type Check
        # 0 if value matches expected type
        transformer_option_expected_types = \
            lambda input_list: [0 if utils.infer_type(input_list[0]) is str else 1,
                                0 if utils.infer_type(input_list[1]) is int else 1,
                                0 if utils.infer_type(input_list[2]) is float else 1,
                                0 if utils.infer_type(input_list[3]) is date else 1,
                                0 if utils.infer_type(input_list[4]) is datetime else 1]

        dt_str: str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        # String, Integer, Float, Date, DateTime. Sample record
        test_input_record_sample_date_str: str = 'this is a string,' + \
                                                 str(utils.generate_random_int_number(10, 100)) + ',' \
                                                 + str(utils.generate_random_int_number(10, 100) * 0.1) + ',' \
                                                 + str(date.today()) + ',' \
                                                 + dt_str
        logger.debug('test_input_record: {}'.format(test_input_record_sample_date_str))

        # Integer, Integer, Float, Date, DateTime
        test_input_record_str: str = str(utils.generate_random_int_number(10, 100)) + ',' + str(
            utils.generate_random_int_number(10, 100)) + ',' + str(
            utils.generate_random_int_number(10, 100) * 0.1) + ',' + str(date.today()) + ',' + dt_str
        logger.debug('test_input_record: {}'.format(test_input_record_str))
        try:

            transformer_options_list: list = list()
            transformer_options_list.append(transformer_option_expected_types)

            result_list: list = getattr(transformers, transformers.LAMBDA_TRANSFORM_METHOD_CHECK)(
                test_input_record_str, transformer_options_list)
            logger.debug('Source Record     : [{}]'.format(test_input_record_str))
            logger.debug('Transformed Record: {}'.format(result_list))
            if sum(i for i in result_list) != 0:
                logger.debug('Null detected')
            else:
                self.fail()
        except Exception as ex:
            logger.error(ex)

    def test_LAMBDA_TRANSFORM_TYPE_CHECK_NULL(self):
        # Transformation. Type Check
        # 0 if value matches expected type
        transformer_option_expected_types = \
            lambda input_list: [0 if utils.infer_type(input_list[0]) is str else 1,
                                0 if utils.infer_type(input_list[1]) is int else 1,
                                0 if utils.infer_type(input_list[2]) is float else 1,
                                0 if utils.infer_type(input_list[3]) is date else 1,
                                0 if utils.infer_type(input_list[4]) is datetime else 1]

        dt_str: str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        # String, Integer, Float, Date, DateTime. Sample record
        test_input_record_sample_date_str: str = 'this is a string,' + \
                                                 str(utils.generate_random_int_number(10, 100)) + ',' + ',' \
                                                 + str(date.today()) + ',' \
                                                 + dt_str
        logger.debug('test_input_record: {}'.format(test_input_record_sample_date_str))
        try:

            transformer_options_list: list = list()
            transformer_options_list.append(transformer_option_expected_types)

            # Transformers will compute if record has the expected types
            result_list: list = getattr(transformers, transformers.LAMBDA_TRANSFORM_METHOD_CHECK)(
                test_input_record_sample_date_str, transformer_options_list)
            logger.debug('Source Record     : [{}]'.format(test_input_record_sample_date_str))
            logger.debug('Transformed Record: {}'.format(result_list))

            # Compute if all columns match ( 0 = all match)
            if sum(i for i in result_list) != 0:
                logger.debug('Record is wrong')
            else:
                self.fail()
        except Exception as ex:
            logger.error(ex)
            self.fail()

    def test_LAMBDA_TRANSFORM_METHOD_REPLACE(self):

        # Transformation. Text Replacement
        transformer_configuration = lambda source_str, to_be_replaced_str, to_replaced_with_str: \
            source_str.replace(to_be_replaced_str, to_replaced_with_str)
        test_input_record_str: str = 'start_record,' + \
                                     str('A' + utils.generate_random_alpha_num_string()) + ',' + \
                                     str('A' + utils.generate_random_alpha_num_string()) + ',' + \
                                     str('A' + utils.generate_random_alpha_num_string()) + ',end_record'
        try:
            to_be_replaced_str: str = 'A'
            to_replaced_with_str: str = 'B'

            transformer_options_list: list = list()
            transformer_options_list.append(transformer_configuration)
            transformer_options_list.append(to_be_replaced_str)
            transformer_options_list.append(to_replaced_with_str)
            result_str: str = getattr(transformers, transformers.LAMBDA_TRANSFORM_METHOD_REPLACE)(
                test_input_record_str, transformer_options_list)
            source_num_of_Bs_int: int = test_input_record_str.count('A') + test_input_record_str.count('B')
            result_num_of_Bs_int: int = result_str.count('B')
            logger.debug('Source Record     : [{}][{}]'.format(test_input_record_str, source_num_of_Bs_int))
            logger.debug('Transformed Record: [{}][{}]'.format(result_str, result_num_of_Bs_int))
            self.assertTrue(result_num_of_Bs_int - source_num_of_Bs_int == 0)

        except Exception as ex:
            logger.error(ex)
            self.fail()

    def test_LAMBDA_TRANSFORM_METHOD_MATH_FORMULA(self):
        # Transformation. Maths Formula and Text Replacement
        transformer_configuration = \
            lambda input_list: str(str(input_list[0])) + "," + \
                               str((float(input_list[1]) + float(input_list[2])) / float(input_list[3])) \
                               + "," + str(input_list[4])
        test_input_record_str: str = 'start_record,' + \
                                     str(utils.generate_random_int_number(10, 100)) + ',' + \
                                     str(utils.generate_random_int_number(10, 100)) + ',' + \
                                     str(utils.generate_random_int_number(10, 100)) + ',end_record'
        try:

            transformer_options_list: list = list()
            transformer_options_list.append(transformer_configuration)

            result_str: str = getattr(transformers, transformers.LAMBDA_TRANSFORM_METHOD_MATH_FORMULA)(
                test_input_record_str, transformer_options_list)
            logger.debug('Source Record     : [{}]'.format(test_input_record_str))
            logger.debug('Transformed Record: [{}]'.format(result_str))
            # Validate that the result is the same
            test_input_record_list: list = test_input_record_str.split(',')
            test_input_float: float = (float(test_input_record_list[1]) + float(test_input_record_list[2])) / float(
                test_input_record_list[3])
            logger.debug('test_input : {}'.format(test_input_float))
            result_float: float = float(result_str.split(',')[1])
            logger.debug('Result: {}'.format(result_float))
            self.assertTrue(test_input_float == result_float)
        except Exception as ex:
            logger.error(ex)
            self.fail()


def tests():
    test_suite = unittest.TestSuite()
    test_suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestTransformers))
    return test_suite


if __name__ == "__main__":
    unittest.TextTestRunner(verbosity=2).run(tests())
