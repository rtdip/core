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

setup(
    name='rtdip-sdk',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url="https://github.com/rtdip/core", 
    classifiers=[
      "License :: OSI Approved :: Apache Software License",
      "Programming Language :: Python :: 3",
      "Programming Language :: Python :: 3.8",
      "Programming Language :: Python :: 3.9",
      "Programming Language :: Python :: 3.10",
    ],
    project_urls={
        "Issue Tracker": "https://github.com/rtdip/core/issues",
        "Source": "https://github.com/rtdip/core/",
        "Documentation": "https://www.rtdip.io/"
    },    
    version=sic(os.environ["RTDIP_SDK_NEXT_VER"]),
    package_dir={'': 'src/sdk/python'},
    packages=find_packages(where='src/sdk/python'),
    python_requires='>=3.8, <4',
    install_requires=['databricks-sql-connector','azure-identity','azure-storage-file-datalake','pyodbc','pandas','jinja2==3.0.3','jinjasql==0.1.8'],
    setup_requires=['pytest-runner','setuptools_scm'],
    tests_require=['pytest'],
    test_suite='tests',
)