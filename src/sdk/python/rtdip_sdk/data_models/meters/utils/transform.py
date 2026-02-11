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

from ..utils import transformers
import logging
import tempfile
import os


def process_file(file_source_name_str: str, transformer_list=None) -> str:
    local_output_file_tmp = tempfile.NamedTemporaryFile(suffix=".csv")
    local_output_file_tmp_str: str = local_output_file_tmp.name
    local_output_file_tmp.close()
    logging.debug("File to process  {}".format(file_source_name_str))
    logging.debug("Created unquoting tmp file: {}".format(local_output_file_tmp.name))
    file_source = open(file_source_name_str, "rt")
    file_target = open(local_output_file_tmp_str, "wt")

    # Do not count the header
    num_of_records_int: int = -1

    ####
    sanitize_map: dict = dict()
    sanitize_map['"'] = ""
    PROCESS_REPLACE = "replace"
    process_definitions: dict = dict()
    process_definitions[PROCESS_REPLACE] = (
        lambda source_str, to_be_replaced_str, to_replaced_with_str: source_str.replace(
            to_be_replaced_str, to_replaced_with_str
        )
    )
    sanitize_function = process_definitions[PROCESS_REPLACE]
    ####

    # transformer
    valid_transformer_bool: bool = False
    header_str: str = None
    if transformer_list is not None:
        if len(transformer_list) == 2:
            transformer_method_str: str = transformer_list[0]
            transformer_options_list = transformer_list[1]
            logging.debug(
                "process_file Transformer method: [{}]".format(transformer_method_str)
            )
            logging.debug(
                "process_file Transformer options: [{}]".format(
                    transformer_options_list
                )
            )
            transformer_options = transformer_options_list[0]
            if len(transformer_options_list) == 2:
                header_str: str = transformer_options_list[1]
                logging.debug(
                    "process_file Transformer header: [{}]".format(header_str)
                )
            valid_transformer_bool = True
        else:
            logging.error("Transformer not valid")
            logging.error(
                "process_file Transformer method: [{}]".format(transformer_list)
            )

    for line in file_source:
        for item_str in sanitize_map:
            line = sanitize_function(line, item_str, sanitize_map[item_str])
            # ignore the header (num_of_records_int >= 0)
            if valid_transformer_bool and num_of_records_int >= 0:
                line = getattr(transformers, transformer_method_str)(
                    line, transformer_options
                )
        if num_of_records_int == -1 and header_str is not None:
            line = header_str.strip().rstrip("\n") + "\n"
        # This will allow transformers that aggregate (emit 1 record each 10)
        if line is not None:
            file_target.write(line)
        num_of_records_int = num_of_records_int + 1
    file_source.close()
    file_target.close()
    logging.debug("Number of records processed: {}".format(num_of_records_int))
    return local_output_file_tmp_str


def remove_file(file_path: str):
    logging.debug("Going to remove: {}".format(file_path))
    if file_path is None:
        logging.error("File path is None")
        return False
    if os.path.exists(file_path):
        try:
            os.remove(file_path)
            logging.debug("[{}] was deleted".format(file_path))
            return True
        except Exception as ex:
            logging.error("Could not remove file: {}".format(file_path))
    else:
        logging.error("File path does not exist: {}".format(file_path))
    return False
