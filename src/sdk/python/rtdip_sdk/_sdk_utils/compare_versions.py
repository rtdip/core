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

from importlib_metadata import version
from semver.version import Version
from packaging.version import Version as PyPIVersion

def _get_semver_from_python_version(pypi_ver: PyPIVersion):
    pre=None
    if pypi_ver.is_prerelease:
        pre = "".join(str(i) for i in pypi_ver.pre)
        pypi_ver = Version(*pypi_ver.release, pre)
    else:
        pypi_ver = Version(*pypi_ver.release)

    return pypi_ver

def _get_python_package_version(package_name):
    pypi_ver = PyPIVersion(version(package_name))
    
    return _get_semver_from_python_version(pypi_ver)

def _package_version_meets_minimum(package_name: str, minimum_version: str) -> bool:
    package_version = _get_python_package_version(package_name)
    package_minimum_version = _get_semver_from_python_version(PyPIVersion(minimum_version))
    version_result =  Version.compare(package_version, package_minimum_version)
    if version_result < 0:
        raise AssertionError("Package {} with version {} does not meet minimum version requirement {}".format(package_name, str(package_version), minimum_version))
    return True