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

from urllib.parse import urlparse
import logging
import re

URI_REGEX: str = "^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\?([^#]*))?(#(.*))?"

AZURE_SCHEME: str = "https"
S3_SCHEME: str = "s3"
GS_SCHEME: str = "gs"
SPARK_S3_SCHEME: str = "s3a"
SCHEMAS: list = [AZURE_SCHEME, S3_SCHEME, GS_SCHEME, SPARK_S3_SCHEME]


def validate_uri(uri: str):
    """
    Validates the uri of a storage object against the supported schemas
    and extracs the scheme, the domain and the path.

    Args:
        uri: The uri to validate.

    Returns:
        Tuple: scheme, domain, path
    """
    if uri is not None:
        try:
            uri = uri.strip()
            if uri.endswith("/"):
                uri = uri[0:-1]
            if not re.search(URI_REGEX, uri):
                logging.error("URI not valid: %s", uri)
                raise SystemError(f"URI not valid: {uri}")
            parsed_uri = urlparse(uri)
            if parsed_uri.scheme in SCHEMAS:
                return parsed_uri.scheme, parsed_uri.hostname, parsed_uri.path
        except Exception as ex:
            logging.error(ex)
    raise SystemError(
        f"Could not convert to valid tuple \
                      or scheme not supported: {uri} {parsed_uri.scheme}"
    )


def get_supported_schema() -> list:
    """
    Returns the schemas that can be validated.
    Returns:
        List with valid schemas
    """
    return SCHEMAS


def to_uri(scheme: str, domain: str, path: str) -> str:
    """
    Returns a fully qualified uri from the provided scheme, domain and path
    Args:
        scheme: The scheme or protocol
        domain: The domain (hostname). Typically this is known as the bucket name
        path: The path. Typically this is known as the keys and the object name
    Returns:
        string: the uri
    """
    return f"{scheme}://{domain}/{path}"
