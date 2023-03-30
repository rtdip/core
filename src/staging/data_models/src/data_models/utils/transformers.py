from data_models.logging import logconf

namespace_str: str = 'transformers'
logger = logconf.get_logger(namespace_str)

LAMBDA_TRANSFORM_METHOD_MATH_FORMULA = 'transform_with_maths_formula'
LAMBDA_TRANSFORM_METHOD_REPLACE = 'transform_with_replace'
LAMBDA_TRANSFORM_METHOD_CHECK = 'transform_with_check'


def transform_with_maths_formula(source_line_str: str, transformer_options: list):
    try:
        transformer_option = transformer_options[0]
        record_to_transform_list: list = source_line_str.split(',')
        return transformer_option(record_to_transform_list)
    except Exception as ex:
        logger.error(ex)
        raise SystemError('Could not process [{}] With Transformer Option [{}]. [{}]'.format(source_line_str,
                                                                                             transformer_option,
                                                                                             repr(ex)))


def transform_with_replace(source_line_str: str, transformer_options: list):
    try:
        transformer_option = transformer_options[0]
        to_be_replaced_str: str = transformer_options[1]
        to_replaced_with_str: str = transformer_options[2]
        return transformer_option(source_line_str, to_be_replaced_str, to_replaced_with_str)
    except Exception as ex:
        logger.error(ex)
        raise SystemError('Could not process [{}] With Transformer Option [{}]. [{}]'.format(source_line_str,
                                                                                             transformer_option,
                                                                                             repr(ex)))


def transform_with_check(source_line_str: str, transformer_options: list):
    try:
        transformer_option = transformer_options[0]
        record_to_transform_list: list = source_line_str.split(',')
        return transformer_option(record_to_transform_list)
    except Exception as ex:
        logger.error(ex)
        raise SystemError('Could not process [{}] With Transformer Option [{}]. [{}]'.format(source_line_str,
                                                                                             transformer_option,
                                                                                             repr(ex)))
