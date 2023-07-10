import boto3
import sys
import random
import string
from moto import mock_s3
import tempfile

import os


from  src.sdk.python.rtdip_sdk.pipelines.utilities.aws.s3_copy_utility import S3CopyUtility
from src.sdk.python.rtdip_sdk.data_models.storage_objects import utils

@mock_s3
def test_s3_copy_utility():

    length: int = 1024
    random.seed()
    rnd_text: str = ''.join(random.choice(string.ascii_lowercase) for _ in range(length))

    with tempfile.NamedTemporaryFile(mode='w', suffix = '.txt', delete=False)  as rnd_tempfile:
        for i in range(length):
            rnd_tempfile.writelines(f'[{i}] - ' + rnd_text)

    rnd_tempfile.close()
    print(rnd_tempfile.name)
    assert(os.path.exists(rnd_tempfile.name))

    letters_and_numbers: string = string.ascii_lowercase + string.digits


    rnd_source_domain_name: str = '.'.join(''.join(random.choice(letters_and_numbers) for _ in range(9)) for _ in range(3))
    rnd_destination_domain_name: str = '.'.join(''.join(random.choice(letters_and_numbers) for _ in range(9)) for _ in range(3))
    rnd_keys: str = ''.join(''.join(random.choice(letters_and_numbers) for _ in range(4)) for _ in range(3))
    rnd_object_name: str = ''.join(random.choice(letters_and_numbers)
     for _ in range(9)) + '.' + ''.join(random.choice(string.ascii_lowercase) for _ in range(3))

    rnd_full_source_s3_uri: str = utils.to_uri(utils.S3_SCHEME, rnd_source_domain_name, rnd_keys + '/' + rnd_object_name)
    rnd_full_destination_s3_uri: str = utils.to_uri(utils.S3_SCHEME, rnd_destination_domain_name, rnd_keys + '/' + rnd_object_name)

    # Create buckets first (required by moto) 
    conn = boto3.resource("s3")
    conn.create_bucket(Bucket=rnd_source_domain_name)
    conn.create_bucket(Bucket=rnd_destination_domain_name)

    # Upload to S3
    s3_copy_utility: S3CopyUtility = S3CopyUtility(rnd_tempfile.name, rnd_full_destination_s3_uri)
    result: bool = s3_copy_utility.execute()
    assert(result is True)

    
    # S3 to S3 Copy
    s3_copy_utility: S3CopyUtility = S3CopyUtility(rnd_full_destination_s3_uri, rnd_full_source_s3_uri)
    result: bool = s3_copy_utility.execute()
    assert(result is True)




    # Download from S3
    # Calculate the hash of the file, delete it and download it from S3 and re-compute the hash and check if it is the same
    os.remove(rnd_tempfile.name)
    s3_copy_utility: S3CopyUtility = S3CopyUtility(rnd_full_destination_s3_uri, rnd_tempfile.name)
    result: bool = s3_copy_utility.execute()
    assert(result is True)
    assert(os.path.exists(rnd_tempfile.name is True))

    


    

