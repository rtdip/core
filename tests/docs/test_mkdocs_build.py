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

import sys

sys.path.insert(0, ".")

from mkdocs.config import load_config
from mkdocs.commands.build import build


def test_mkdocs_build():
    mkdocs_config = load_config(strict=True)
    mkdocs_config["plugins"].run_event("startup", command="build", dirty=False)
    try:
        build(mkdocs_config)
    finally:
        mkdocs_config["plugins"].run_event("shutdown")
