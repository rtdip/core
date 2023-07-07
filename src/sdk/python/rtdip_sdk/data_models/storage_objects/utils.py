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
import re

URI_REGEX: str = "^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\?([^#]*))?(#(.*))?"


AZURE_PROTOCOL: str = "https://"
S3_PROTOCOL: str = "s3://"

PROTOCOLS: list = [AZURE_PROTOCOL, S3_PROTOCOL]


def validate_uri(uri_str: str):
    if uri_str is not None:
        try:
            uri_str = uri_str.strip()
            if not re.search(URI_REGEX, uri_str):
                logging.error('URI not valid: {}'.format(uri_str))
                raise SystemError('URI not valid: {}'.format(uri_str))
            if uri_str.startswith(S3_PROTOCOL):
                if uri_str.endswith('/'):
                    uri_str = uri_str[0:-1]
                keys: list = uri_str.replace(S3_PROTOCOL, '').split('/')
                return S3_PROTOCOL, keys
        except Exception as ex:
            logging.error(ex)
    raise SystemError('Could not convert to valid storage object: {}'.format(uri_str))
