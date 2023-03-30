from data_models.utils import transformers
from data_models.logging import logconf
import tempfile
import os


namespace_str: str = 'Transform'
logger = logconf.get_logger(namespace_str)


# TODO: Checks and exceptions
# TODO: Returning tmp file
def process_file(file_source_name_str: str, transformer_list=None) -> str:
    local_output_file_tmp = tempfile.NamedTemporaryFile(suffix='.csv')
    local_output_file_tmp_str: str = local_output_file_tmp.name
    local_output_file_tmp.close()
    logger.debug('File to process  {}'.format(file_source_name_str))
    logger.debug('Created unquoting tmp file: {}'.format(local_output_file_tmp.name))
    file_source = open(file_source_name_str, "rt")
    file_target = open(local_output_file_tmp_str, "wt")

    # Do not count the header
    num_of_records_int: int = -1

    ####
    sanitize_map: dict = dict()
    sanitize_map['"'] = ''
    PROCESS_REPLACE = 'replace'
    process_definitions: dict = dict()
    process_definitions[PROCESS_REPLACE] = lambda source_str, to_be_replaced_str, to_replaced_with_str: \
        source_str.replace(to_be_replaced_str, to_replaced_with_str)
    sanitize_function = process_definitions[PROCESS_REPLACE]
    ####

    # transformer
    valid_transformer_bool: bool = False
    header_str: str = None
    if transformer_list is not None:
        if len(transformer_list) == 2:
            transformer_method_str: str = transformer_list[0]
            transformer_options_list = transformer_list[1]
            logger.debug('process_file Transformer method: [{}]'.format(transformer_method_str))
            logger.debug('process_file Transformer options: [{}]'.format(transformer_options_list))
            transformer_options = transformer_options_list[0]
            if len(transformer_options_list) == 2:
                header_str: str = transformer_options_list[1]
                logger.debug('process_file Transformer header: [{}]'.format(header_str))
            valid_transformer_bool = True
        else:
            logger.error('Transformer not valid')
            logger.error('process_file Transformer method: [{}]'.format(transformer_list))

    for line in file_source:
        for item_str in sanitize_map:
            line = sanitize_function(line, item_str, sanitize_map[item_str])
            # ignore the header (num_of_records_int >= 0)
            if valid_transformer_bool and num_of_records_int >= 0:
                line = getattr(transformers, transformer_method_str)(line, transformer_options)
        if num_of_records_int == -1 and header_str is not None:
            line = header_str.strip().rstrip('\n') + '\n'
        # This will allow transformers that aggregate (emit 1 record each 10)
        if line is not None:
            file_target.write(line)
        num_of_records_int = num_of_records_int + 1
    file_source.close()
    file_target.close()
    logger.debug('Number of records processed: {}'.format(num_of_records_int))
    # Swap file names
    # logger.debug('Going to rename: {}'.format(file_source_name_str))
    # remove_file(file_source_name_str)
    # os.rename(local_output_file_tmp_str, file_source_name_str)
    return local_output_file_tmp_str


def remove_file(file_path: str):
    logger.debug('Going to remove: {}'.format(file_path))
    if file_path is None:
        logger.error('File path is None')
        return False
    if os.path.exists(file_path):
        try:
            os.remove(file_path)
            logger.debug('[{}] was deleted'.format(file_path))
            return True
        except Exception as ex:
            logger.error('Could not remove file: {}'.format(file_path))
    else:
        logger.error('File path does not exist: {}'.format(file_path))
    return False