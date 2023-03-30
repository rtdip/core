from data_models.utils import transformers
import datetime
import hashlib
import time

series_id_str = 'usage_series_id_001'
output_header_str: str = 'uid,series_id,timestamp,interval_timestamp,value'
transformer_method_str: str = transformers.LAMBDA_TRANSFORM_METHOD_CHECK


def anonymizer(input_str: str) -> str:
    result = hashlib.md5(input_str.encode())
    return str(result.hexdigest())


transformer_configuration = \
            lambda input_list: str(anonymizer(input_list[0])) + ',' + series_id_str + '_' + input_list[1] + ',' + \
                               str(int(time.mktime(datetime.datetime.strptime(str(input_list[2]).replace(".0", "."),
                                                                          "%Y-%m-%d %H:%M:%S.%f").timetuple()))) + ',' + \
                               str(int(time.mktime(datetime.datetime.strptime(str(input_list[2]).replace(".0", "."),
                                                                          "%Y-%m-%d %H:%M:%S.%f").timetuple()))) + ',' + \
                               str(float(input_list[3])).replace(' ', '') + '\n'