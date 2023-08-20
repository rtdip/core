# Copyright 2022 RTDIP
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
from pathlib import Path
import shutil
from dataclasses import dataclass


@dataclass
class FileInfoFixture:
    """
    This class mocks the DBUtils FileInfo object
    """

    path: str
    name: str
    size: int
    modificationTime: int  # NOSONAR


class DBUtilsFSFixture:
    """
    This class is used for mocking the behavior of DBUtils inside tests.
    """

    def __init__(self):
        self.fs = self

    def cp(self, src: str, dest: str, recurse: bool = False):
        copy_func = shutil.copytree if recurse else shutil.copy
        copy_func(src, dest)

    def ls(self, path: str):
        _paths = Path(path).glob("*")
        _objects = [
            FileInfoFixture(
                str(p.absolute()), p.name, p.stat().st_size, int(p.stat().st_mtime)
            )
            for p in _paths
        ]
        return _objects

    def mkdirs(self, path: str):
        Path(path).mkdir(parents=True, exist_ok=True)

    def mv(self, src: str, dest: str, recurse: bool = False):
        copy_func = shutil.copytree if recurse else shutil.copy
        shutil.move(src, dest, copy_function=copy_func)

    def put(self, path: str, content: str, overwrite: bool = False):
        _f = Path(path)

        if _f.exists() and not overwrite:
            raise FileExistsError("File already exists")

        _f.write_text(content, encoding="utf-8")

    def rm(self, path: str, recurse: bool = False):
        deletion_func = shutil.rmtree if recurse else os.remove
        deletion_func(path)


class DBUtilsSecretsFixture:
    """
    This class is used for mocking the behavior of DBUtils inside tests.
    """

    def __init__(self, secret_value):
        self.secrets = self
        self.secret_value = secret_value

    def get(self, scope: str, key: str):
        assert type(scope) == str
        assert type(key) == str
        return self.secret_value
