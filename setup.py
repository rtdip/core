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

"""A setuptools based setup module.
See:
https://packaging.python.org/guides/distributing-packages-using-setuptools/
https://github.com/pypa/sampleproject
"""

# Always prefer setuptools over distutils
from setuptools import setup, find_packages, sic
from setuptools.extern import packaging
import pathlib
import os

here = pathlib.Path(__file__).parent.resolve()

long_description = (here / "PYPI-README.md").read_text()

INSTALL_REQUIRES = [
    "databricks-sql-connector==2.9.3",
    "azure-identity==1.12.0",
    "pandas>=1.5.2,<3.0.0",
    "jinja2==3.1.2",
    "importlib_metadata>=1.0.0",
    "semver==3.0.0",
    "xlrd==2.0.1",
    "grpcio>=1.48.1",
    "grpcio-status>=1.48.1",
    "googleapis-common-protos>=1.56.4",
    "langchain==0.0.291",
    "openai==0.27.8",
    "sqlparams==5.1.0",
    "entsoe-py==0.5.10",
]

PYSPARK_PACKAGES = [
    "pyspark>=3.3.0,<3.6.0",
    "delta-spark>=2.2.0,<3.1.0",
]

PIPELINE_PACKAGES = [
    "dependency-injector==4.41.0",
    "databricks-sdk==0.9.0",
    "pydantic==2.4.2",
    "azure-storage-file-datalake==12.12.0",
    "azure-mgmt-storage==21.0.0",
    "azure-mgmt-eventgrid==10.2.0",
    "boto3==1.28.2",
    "hvac==1.1.1",
    "azure-keyvault-secrets==4.7.0",
    "web3==6.5.0",
    "polars[deltalake]==0.18.8",
    "delta-sharing==1.0.0",
    "xarray>=2023.1.0,<2023.8.0",
    "ecmwf-api-client==1.6.3",
    "netCDF4==1.6.4",
    "joblib==1.3.2",
]

EXTRAS_DEPENDENCIES: dict[str, list[str]] = {
    "pipelines": PIPELINE_PACKAGES,
    "pyspark": PYSPARK_PACKAGES,
}

setup(
    name="rtdip-sdk",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/rtdip/core",
    classifiers=[
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    project_urls={
        "Issue Tracker": "https://github.com/rtdip/core/issues",
        "Source": "https://github.com/rtdip/core/",
        "Documentation": "https://www.rtdip.io/",
    },
    version=sic(os.environ["RTDIP_SDK_NEXT_VER"]),
    package_dir={"": "src/sdk/python"},
    include_package_data=True,
    packages=find_packages(where="src/sdk/python"),
    python_requires=">=3.9, <3.12",
    install_requires=INSTALL_REQUIRES,
    extras_require=EXTRAS_DEPENDENCIES,
    setup_requires=["pytest-runner", "setuptools_scm"],
    tests_require=["pytest"],
    test_suite="tests",
)
