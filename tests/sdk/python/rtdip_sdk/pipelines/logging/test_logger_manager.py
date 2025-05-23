# Copyright 2025 RTDIP
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
from src.sdk.python.rtdip_sdk.pipelines.logging.logger_manager import LoggerManager


def test_logger_manager_basic_function():
    logger_manager = LoggerManager()
    logger1 = logger_manager.create_logger("logger1")
    assert logger1 is logger_manager.get_logger("logger1")

    assert logger_manager.get_logger("logger2") is None


def test_singleton_functionality():
    logger_manager = LoggerManager()
    logger_manager2 = LoggerManager()

    assert logger_manager is logger_manager2
