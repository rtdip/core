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
sys.path.insert(0, '.')
import logging
import random
import string

from src.sdk.python.rtdip_sdk.data_models.storage_objects import StorageObject
from src.sdk.python.rtdip_sdk.data_models.storage_objects import utils

format_str: str = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
date_format_str: str = '%m/%d/%Y %H:%M:%S %Z'
logging.basicConfig(format=format_str,
                    datefmt=date_format_str,
                    level=logging.INFO)

logger = logging.getLogger('test_storage_object')



def test_storage_object():
   

    random.seed()
    rnd_domain_name: str = '.'.join(''.join(random.choice(string.ascii_uppercase + string.ascii_lowercase
                                                          + string.digits) for _ in range(9)) for _ in range(3))
    rnd_keys: str = '/'.join(''.join(random.choice(string.ascii_uppercase + string.ascii_lowercase
                                                          + string.digits) for _ in range(4)) for _ in range(3))
   

    rnd_object_name: str = ''.join(
        random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits)
        for _ in range(9)) + '.' + ''.join(random.choice(string.ascii_lowercase) for _ in range(3))

  

    rnd_full_s3_uri: str = StorageObject.S3_PROTOCOL + rnd_domain_name + '/' + rnd_keys \
                                + '/' + rnd_object_name


    logger.info('Started')
    logger.debug('{}'.format(rnd_full_s3_uri))
