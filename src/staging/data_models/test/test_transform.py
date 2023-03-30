#!/usr/bin/env python
from data_models.transformers import london_smart_meter_transformer_2_usage
from data_models.logging import logconf
from data_models.utils import transform
import pandas as pd
import unittest
import numpy
import os

namespace_str: str = 'test_transformers'
logger = logconf.get_logger(namespace_str)

test_data_smart_meters_london_dir: \
    list = [os.path.join('test_data', 'smart_meters_london')]


def get_test_data_directory(path_list: list) -> str:
    current_dir: str = os.path.dirname(os.path.realpath(__file__))
    logger.debug('Current Dir: %s', current_dir)
    os.chdir(current_dir)
    dir_full_path_str: str = os.path.join(current_dir, *path_list)
    logger.debug('Checking if dir exist: %s', dir_full_path_str)
    if os.path.exists(dir_full_path_str):
        logger.debug('\tDir exist: %s', dir_full_path_str)
        return dir_full_path_str
    return None


class TestTransformers(unittest.TestCase):

    def setUp(self):
        pass

    def test_transformer_smart_meter_london_2_LF_Energy_Usage(self):
        logger.debug('Sample data set from (full data set): '
                     '{}{}'.format("""\t\nhttps://www.kaggle.com/datasets/jeanmidev/smart-meters-in-london""",
                                   """\t\nhttps://data.london.gov.uk/dataset/smartmeter-energy-use-data-in-london-households"""))

        # Configure Source (local file)
        test_data_dir_list: list = test_data_smart_meters_london_dir
        logger.debug(test_data_dir_list)
        csv_input_file_cc_lcl_fulldata_str: str = os.path.join(get_test_data_directory(test_data_dir_list),
                                                                   "CC_LCL-FullData.csv")
        logger.debug('Test File: {}'.format(csv_input_file_cc_lcl_fulldata_str))
        self.assertTrue(os.path.isfile(csv_input_file_cc_lcl_fulldata_str))

        # Transformer Configuration
        # Transformer method
        transformer_method_str: str = london_smart_meter_transformer_2_usage.transformer_method_str

        transformer_configuration = london_smart_meter_transformer_2_usage.transformer_configuration
        output_header_str: str = london_smart_meter_transformer_2_usage.output_header_str
        transformer_options_list: list = [[transformer_configuration], output_header_str]
        transformer_list: list = [transformer_method_str, transformer_options_list]
        csv_output_file_str: str = transform.process_file(csv_input_file_cc_lcl_fulldata_str, transformer_list)
        logger.debug('Result: {}'.format(csv_output_file_str))

        try:
            source_file_df = pd.read_csv(csv_input_file_cc_lcl_fulldata_str)
            transformed_file_df = pd.read_csv(csv_output_file_str)
            logger.debug('\nSource File:\n ' + source_file_df.head(50).to_string())
            logger.debug('\nTransformed File:\n' + transformed_file_df.head(50).to_string())
            logger.debug(len(transformed_file_df.columns))
            logger.debug(len(output_header_str.split(',')))

            self.assertTrue(len(transformed_file_df.columns) == len(output_header_str.split(',')))
            sample_df = transformed_file_df.sample()
            sample_col = sample_df.iloc[:, 2].sum()
            self.assertTrue(type(sample_col) == numpy.int64)
        except Exception as ex:
            logger.error(ex)
            self.fail()

        #logger.debug('Result from deleting file [{}]: {}'.format(csv_output_file_str,
        #                                                         transform.remove_file(csv_output_file_str)))

    def tearDown(self):
        pass


def tests():
    test_suite = unittest.TestSuite()
    test_suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestTransformers))
    return test_suite


if __name__ == "__main__":
    unittest.TextTestRunner(verbosity=2).run(tests())
