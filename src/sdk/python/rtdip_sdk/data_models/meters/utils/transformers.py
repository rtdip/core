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


import logging

LAMBDA_TRANSFORM_METHOD_MATH_FORMULA = "transform_with_maths_formula"
LAMBDA_TRANSFORM_METHOD_REPLACE = "transform_with_replace"
LAMBDA_TRANSFORM_METHOD_CHECK = "transform_with_check"


def transform_with_maths_formula(source_line_str: str, transformer_options: list):
    try:
        transformer_option = transformer_options[0]
        record_to_transform_list: list = source_line_str.split(",")
        return transformer_option(record_to_transform_list)
    except Exception as ex:
        logging.error(ex)
        raise SystemError(
            "Could not process [{}] With Transformer Option [{}]. [{}]".format(
                source_line_str, transformer_option, repr(ex)
            )
        )


def transform_with_replace(source_line_str: str, transformer_options: list):
    try:
        transformer_option = transformer_options[0]
        to_be_replaced_str: str = transformer_options[1]
        to_replaced_with_str: str = transformer_options[2]
        return transformer_option(
            source_line_str, to_be_replaced_str, to_replaced_with_str
        )
    except Exception as ex:
        logging.error(ex)
        raise SystemError(
            "Could not process [{}] With Transformer Option [{}]. [{}]".format(
                source_line_str, transformer_option, repr(ex)
            )
        )


def transform_with_check(source_line_str: str, transformer_options: list):
    try:
        transformer_option = transformer_options[0]
        record_to_transform_list: list = source_line_str.split(",")
        return transformer_option(record_to_transform_list)
    except Exception as ex:
        logging.error(ex)
        raise SystemError(
            "Could not process [{}] With Transformer Option [{}]. [{}]".format(
                source_line_str, transformer_option, repr(ex)
            )
        )
