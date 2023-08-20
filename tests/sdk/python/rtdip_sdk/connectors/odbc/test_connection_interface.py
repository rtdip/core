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
from src.sdk.python.rtdip_sdk.connectors.connection_interface import ConnectionInterface


class TestConnection(ConnectionInterface):
    def __init__(self):
        pass  # passing because method does not need to be not implemented

    def close(self):
        raise NotImplementedError

    def cursor(self):
        raise NotImplementedError


def test_close_method_missing():
    class TestConnectionClose(ConnectionInterface):
        def __init__(self):
            pass  # passing because method does not need to be not implemented

        def cursor(self):
            raise NotImplementedError

    with pytest.raises(TypeError):
        TestConnectionClose()


def test_cursor_method_missing():
    class TestConnectionCursor(ConnectionInterface):
        def __init__(self):
            pass  # passing because method does not need to be not implemented

        def close(self):
            raise NotImplementedError

    with pytest.raises(TypeError):
        TestConnectionCursor()


def test_close_method():
    test_connection = TestConnection()
    with pytest.raises(NotImplementedError):
        test_connection.close()


def test_cursor_method():
    test_connection = TestConnection()
    with pytest.raises(NotImplementedError):
        test_connection.cursor()
