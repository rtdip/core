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

from importlib_metadata import version as metadata_version
from semver.version import Version

def _version_meets_minimum(package_name: str, minimum_version: str) -> bool:
    package_version = Version.parse(metadata_version(package_name))
    version_result =  Version.compare(package_version, minimum_version)
    if version_result < 0:
        raise AssertionError("Package {} does not meet minimum version requirement {}".format(package_name, minimum_version))
    return True