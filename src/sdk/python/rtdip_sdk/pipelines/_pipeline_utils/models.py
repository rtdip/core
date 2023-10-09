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

from enum import Enum
from typing import List, Optional
from pydantic.v1 import BaseModel


class SystemType(Enum):
    """The type of the system."""

    # Executable in a python environment
    PYTHON = 1
    # Executable in a pyspark environment
    PYSPARK = 2
    # Executable in a databricks environment
    PYSPARK_DATABRICKS = 3


class LibraryTypes(Enum):
    MAVEN = 1
    PYPI = 2
    PYTHONWHL = 3


class MavenLibrary(BaseModel):
    group_id: str
    artifact_id: str
    version: str
    repo: Optional[str]

    def to_string(self) -> str:
        return f"{self.group_id}:{self.artifact_id}:{self.version}"


class PyPiLibrary(BaseModel):
    name: str
    version: str
    repo: Optional[str]

    def to_string(self) -> str:
        return f"{self.name}=={self.version}"


class PythonWheelLibrary(BaseModel):
    path: str


class Libraries(BaseModel):
    maven_libraries: List[MavenLibrary] = []
    pypi_libraries: List[PyPiLibrary] = []
    pythonwheel_libraries: List[PythonWheelLibrary] = []

    def add_maven_library(self, maven_library: MavenLibrary):
        self.maven_libraries.append(maven_library)

    def add_pypi_library(self, pypi_library: PyPiLibrary):
        self.pypi_libraries.append(pypi_library)

    def add_pythonwhl_library(self, whl_library: PythonWheelLibrary):
        self.pythonwheel_libraries.append(whl_library)

    def get_libraries_from_components(self, component_list: list):
        for component in component_list:
            component_libraries = component.libraries()
            for library in component_libraries.pypi_libraries:
                self.add_pypi_library(library)
            for library in component_libraries.maven_libraries:
                self.add_maven_library(library)
            for library in component_libraries.pythonwheel_libraries:
                self.add_pythonwhl_library(library)
