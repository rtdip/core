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

import os
from importlib_metadata import version
from semver.version import Version
from packaging.version import Version as PyPIVersion


def _get_databricks_package_versions(databricks_runtime_version, package_name):
    if package_name == "pyspark":
        from databricks.sdk.runtime import spark

        return spark.version
    elif package_name == "delta-spark":
        if (
            Version.compare(
                databricks_runtime_version,
                Version.parse("13.2", optional_minor_and_patch=True),
            )
            >= 0
        ):
            return "3.0.0"
        elif (
            Version.compare(
                databricks_runtime_version,
                Version.parse("13.1", optional_minor_and_patch=True),
            )
            >= 0
        ):
            return "2.4.0"
        elif (
            Version.compare(
                databricks_runtime_version,
                Version.parse("13.0", optional_minor_and_patch=True),
            )
            >= 0
        ):
            return "2.3.0"
        elif (
            Version.compare(
                databricks_runtime_version,
                Version.parse("12.1", optional_minor_and_patch=True),
            )
            >= 0
        ):
            return "2.2.0"
        elif (
            Version.compare(
                databricks_runtime_version,
                Version.parse("11.3", optional_minor_and_patch=True),
            )
            >= 0
        ):
            return "2.1.0"
        elif (
            Version.compare(
                databricks_runtime_version,
                Version.parse("10.4", optional_minor_and_patch=True),
            )
            >= 0
        ):
            return "1.1.0"


def _get_package_version(package_name):
    if "DATABRICKS_RUNTIME_VERSION" in os.environ and package_name in [
        "delta-spark",
        "pyspark",
    ]:
        return _get_databricks_package_versions(
            Version.parse(
                os.environ.get("DATABRICKS_RUNTIME_VERSION"),
                optional_minor_and_patch=True,
            ),
            package_name,
        )
    else:
        return version(package_name)


def _get_semver_from_python_version(pypi_ver: PyPIVersion):
    pre = None
    if pypi_ver.is_prerelease:
        pre = "".join(str(i) for i in pypi_ver.pre)
        pypi_ver = Version(*pypi_ver.release, pre)
    else:
        pypi_ver = Version(*pypi_ver.release)

    return pypi_ver


def _get_python_package_version(package_name):
    pypi_ver = PyPIVersion(_get_package_version(package_name))

    return _get_semver_from_python_version(pypi_ver)


def _package_version_meets_minimum(package_name: str, minimum_version: str) -> bool:
    package_version = _get_python_package_version(package_name)
    package_minimum_version = _get_semver_from_python_version(
        PyPIVersion(minimum_version)
    )
    version_result = Version.compare(package_version, package_minimum_version)
    if version_result < 0:
        raise AssertionError(
            "Package {} with version {} does not meet minimum version requirement {}".format(
                package_name, str(package_version), minimum_version
            )
        )
    return True
