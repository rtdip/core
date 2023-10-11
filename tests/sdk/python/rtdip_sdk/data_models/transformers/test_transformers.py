# Copyright 2022 RTDIP
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import sys

sys.path.insert(0, ".")


from src.sdk.python.rtdip_sdk.data_models.meters.utils import transformers
from src.sdk.python.rtdip_sdk.data_models.utils import timeseries_utils
from datetime import date
import datetime

import logging
import pytest


def test_LAMBDA_TRANSFORM_TYPE_CHECK():
    # Transformation. Type Check
    # 0 if value matches expected type
    transformer_option_expected_types = lambda input_list: [
        0 if timeseries_utils.infer_type(input_list[0]) is str else 1,
        0 if timeseries_utils.infer_type(input_list[1]) is int else 1,
        0 if timeseries_utils.infer_type(input_list[2]) is float else 1,
        0 if timeseries_utils.infer_type(input_list[3]) is date else 1,
        0 if timeseries_utils.infer_type(input_list[4]) is datetime.datetime else 1,
    ]

    ###

    ###

    dt_str: str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

    # String, Integer, Float, Date, DateTime. Sample record
    test_input_record_sample_date_str: str = (
        "this is a string,"
        + str(timeseries_utils.generate_random_int_number(10, 100))
        + ","
        + str(timeseries_utils.generate_random_int_number(10, 100) * 0.1)
        + ","
        + str(date.today())
        + ","
        + dt_str
    )
    logging.debug("test_input_record: {}".format(test_input_record_sample_date_str))
    try:
        transformer_options_list: list = list()
        transformer_options_list.append(transformer_option_expected_types)

        # Transformers will compute if record has the expected types
        result_list: list = getattr(
            transformers, transformers.LAMBDA_TRANSFORM_METHOD_CHECK
        )(test_input_record_sample_date_str, transformer_options_list)
        logging.debug(
            "Source Record     : [{}]".format(test_input_record_sample_date_str)
        )
        logging.debug("Transformed Record: {}".format(result_list))

        # Compute if all columns match ( 0 = all match)
        if sum(i for i in result_list) == 0:
            logging.debug("Record has the right types")
        else:
            pytest.fail()
    except Exception as ex:
        logging.error(ex)
        pytest.fail()


def test_LAMBDA_TRANSFORM_TYPES_AND_RANGE_CHECK():
    # Transformation. Type Check
    # 0 if value matches expected type
    transformer_configuration = lambda input_list: [
        0 if timeseries_utils.infer_type(input_list[0]) is str else 1,
        0 if (0 < int(input_list[1]) < 10) else 1,
        0 if timeseries_utils.infer_type(input_list[2]) is float else 1,
        0 if timeseries_utils.infer_type(input_list[3]) is date else 1,
        0 if timeseries_utils.infer_type(input_list[4]) is datetime.datetime else 1,
    ]

    dt_str: str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    # String, Integer, Float, Date, DateTime. Sample record
    expected_types_list: list = [str, int, float, date, datetime]
    logging.debug(expected_types_list)
    test_input_record_sample_date_str: str = (
        "this is a string,"
        + "9"
        + ","
        + str(timeseries_utils.generate_random_int_number(10, 100) * 0.1)
        + ","
        + str(date.today())
        + ","
        + dt_str
    )
    logging.debug("test_input_record: {}".format(test_input_record_sample_date_str))
    try:
        transformer_options_list: list = list()
        transformer_options_list.append(transformer_configuration)

        # Transformers will compute if record has the expected types
        result_list: list = getattr(
            transformers, transformers.LAMBDA_TRANSFORM_METHOD_CHECK
        )(test_input_record_sample_date_str, transformer_options_list)
        logging.debug("Expected source types: {}".format(expected_types_list))
        logging.debug(
            "Source Record     : [{}]".format(test_input_record_sample_date_str)
        )
        logging.debug("Transformed Record: {}".format(result_list))

        # Compute if all columns match ( 0 = all match)
        if sum(i for i in result_list) == 0:
            logging.debug("Record has the right types")
        else:
            pytest.fail()
    except Exception as ex:
        logging.error(ex)
        pytest.fail()


def test_LAMBDA_TRANSFORM_TYPE_CHECK_one_wrong_type():
    # Transformation. Type Check
    # 0 if value matches expected type
    transformer_option_expected_types = lambda input_list: [
        0 if timeseries_utils.infer_type(input_list[0]) is str else 1,
        0 if timeseries_utils.infer_type(input_list[1]) is int else 1,
        0 if timeseries_utils.infer_type(input_list[2]) is float else 1,
        0 if timeseries_utils.infer_type(input_list[3]) is date else 1,
        0 if timeseries_utils.infer_type(input_list[4]) is datetime else 1,
    ]

    dt_str: str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    # String, Integer, Float, Date, DateTime. Sample record
    test_input_record_sample_date_str: str = (
        "this is a string,"
        + str(timeseries_utils.generate_random_int_number(10, 100))
        + ","
        + str(timeseries_utils.generate_random_int_number(10, 100) * 0.1)
        + ","
        + str(date.today())
        + ","
        + dt_str
    )
    logging.debug("test_input_record: {}".format(test_input_record_sample_date_str))

    # Integer, Integer, Float, Date, DateTime
    test_input_record_str: str = (
        str(timeseries_utils.generate_random_int_number(10, 100))
        + ","
        + str(timeseries_utils.generate_random_int_number(10, 100))
        + ","
        + str(timeseries_utils.generate_random_int_number(10, 100) * 0.1)
        + ","
        + str(date.today())
        + ","
        + dt_str
    )
    logging.debug("test_input_record: {}".format(test_input_record_str))
    try:
        transformer_options_list: list = list()
        transformer_options_list.append(transformer_option_expected_types)

        result_list: list = getattr(
            transformers, transformers.LAMBDA_TRANSFORM_METHOD_CHECK
        )(test_input_record_str, transformer_options_list)
        logging.debug("Source Record     : [{}]".format(test_input_record_str))
        logging.debug("Transformed Record: {}".format(result_list))
        if sum(i for i in result_list) != 0:
            logging.debug("Null detected")
        else:
            pytest.fail()
    except Exception as ex:
        logging.error(ex)


def test_LAMBDA_TRANSFORM_TYPE_CHECK_NULL():
    # Transformation. Type Check
    # 0 if value matches expected type
    transformer_option_expected_types = lambda input_list: [
        0 if timeseries_utils.infer_type(input_list[0]) is str else 1,
        0 if timeseries_utils.infer_type(input_list[1]) is int else 1,
        0 if timeseries_utils.infer_type(input_list[2]) is float else 1,
        0 if timeseries_utils.infer_type(input_list[3]) is date else 1,
        0 if timeseries_utils.infer_type(input_list[4]) is datetime else 1,
    ]

    dt_str: str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    # String, Integer, Float, Date, DateTime. Sample record
    test_input_record_sample_date_str: str = (
        "this is a string,"
        + str(timeseries_utils.generate_random_int_number(10, 100))
        + ","
        + ","
        + str(date.today())
        + ","
        + dt_str
    )
    logging.debug("test_input_record: {}".format(test_input_record_sample_date_str))
    try:
        transformer_options_list: list = list()
        transformer_options_list.append(transformer_option_expected_types)

        # Transformers will compute if record has the expected types
        result_list: list = getattr(
            transformers, transformers.LAMBDA_TRANSFORM_METHOD_CHECK
        )(test_input_record_sample_date_str, transformer_options_list)
        logging.debug(
            "Source Record     : [{}]".format(test_input_record_sample_date_str)
        )
        logging.debug("Transformed Record: {}".format(result_list))

        # Compute if all columns match ( 0 = all match)
        if sum(i for i in result_list) != 0:
            logging.debug("Record is wrong")
        else:
            pytest.fail()
    except Exception as ex:
        logging.error(ex)
        pytest.fail()


def test_LAMBDA_TRANSFORM_METHOD_REPLACE():
    # Transformation. Text Replacement
    transformer_configuration = (
        lambda source_str, to_be_replaced_str, to_replaced_with_str: source_str.replace(
            to_be_replaced_str, to_replaced_with_str
        )
    )
    test_input_record_str: str = (
        "start_record,"
        + str("A" + timeseries_utils.generate_random_alpha_num_string())
        + ","
        + str("A" + timeseries_utils.generate_random_alpha_num_string())
        + ","
        + str("A" + timeseries_utils.generate_random_alpha_num_string())
        + ",end_record"
    )
    try:
        to_be_replaced_str: str = "A"
        to_replaced_with_str: str = "B"

        transformer_options_list: list = list()
        transformer_options_list.append(transformer_configuration)
        transformer_options_list.append(to_be_replaced_str)
        transformer_options_list.append(to_replaced_with_str)
        result_str: str = getattr(
            transformers, transformers.LAMBDA_TRANSFORM_METHOD_REPLACE
        )(test_input_record_str, transformer_options_list)
        source_num_of_Bs_int: int = test_input_record_str.count(
            "A"
        ) + test_input_record_str.count("B")
        result_num_of_Bs_int: int = result_str.count("B")
        logging.debug(
            "Source Record     : [{}][{}]".format(
                test_input_record_str, source_num_of_Bs_int
            )
        )
        logging.debug(
            "Transformed Record: [{}][{}]".format(result_str, result_num_of_Bs_int)
        )
        assert result_num_of_Bs_int - source_num_of_Bs_int == 0

    except Exception as ex:
        logging.error(ex)
        pytest.fail()


def test_LAMBDA_TRANSFORM_METHOD_MATH_FORMULA():
    # Transformation. Maths Formula and Text Replacement
    transformer_configuration = (
        lambda input_list: str(str(input_list[0]))
        + ","
        + str((float(input_list[1]) + float(input_list[2])) / float(input_list[3]))
        + ","
        + str(input_list[4])
    )
    test_input_record_str: str = (
        "start_record,"
        + str(timeseries_utils.generate_random_int_number(10, 100))
        + ","
        + str(timeseries_utils.generate_random_int_number(10, 100))
        + ","
        + str(timeseries_utils.generate_random_int_number(10, 100))
        + ",end_record"
    )
    try:
        transformer_options_list: list = list()
        transformer_options_list.append(transformer_configuration)

        result_str: str = getattr(
            transformers, transformers.LAMBDA_TRANSFORM_METHOD_MATH_FORMULA
        )(test_input_record_str, transformer_options_list)
        logging.debug("Source Record     : [{}]".format(test_input_record_str))
        logging.debug("Transformed Record: [{}]".format(result_str))
        # Validate that the result is the same
        test_input_record_list: list = test_input_record_str.split(",")
        test_input_float: float = (
            float(test_input_record_list[1]) + float(test_input_record_list[2])
        ) / float(test_input_record_list[3])
        logging.debug("test_input : {}".format(test_input_float))
        result_float: float = float(result_str.split(",")[1])
        logging.debug("Result: {}".format(result_float))
        assert test_input_float == result_float
    except Exception as ex:
        logging.error(ex)
        pytest.fail()
