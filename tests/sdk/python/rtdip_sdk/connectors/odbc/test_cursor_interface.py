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

import pytest
from src.sdk.python.rtdip_sdk.connectors.cursor_interface import CursorInterface


class TestCursor(CursorInterface):
    def execute(self, query: str):
        raise NotImplementedError

    def fetch_all(self):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError


def test_execute_method_missing():
    class TestCursorExecute(CursorInterface):
        def __init__(self):
            pass  # passing because method does not need to be not implemented

        def fetch_all(self):
            raise NotImplementedError

        def close(self):
            raise NotImplementedError

    with pytest.raises(TypeError):
        TestCursorExecute()


def test_fetch_all_method_missing():
    class TestCursorFetchAll(CursorInterface):
        def __init__(self):
            pass  # passing because method does not need to be not implemented

        def execute(self, query: str):
            raise NotImplementedError

        def close(self):
            raise NotImplementedError

    with pytest.raises(TypeError):
        TestCursorFetchAll()


def test_close_method_missing():
    class TestCursorFetchAll(CursorInterface):
        def __init__(self):
            pass  # passing because method does not need to be not implemented

        def execute(self, query: str):
            raise NotImplementedError

        def fetch_all(self):
            raise NotImplementedError

    with pytest.raises(TypeError):
        TestCursorFetchAll()


def test_execute_method():
    test_cursor = TestCursor()
    query = "test"
    with pytest.raises(NotImplementedError):
        test_cursor.execute(query)


def test_fetch_all_method():
    test_cursor = TestCursor()
    with pytest.raises(NotImplementedError):
        test_cursor.fetch_all()


def test_close_method():
    test_cursor = TestCursor()
    with pytest.raises(NotImplementedError):
        test_cursor.close()
