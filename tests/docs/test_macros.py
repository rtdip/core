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
import os
from datetime import datetime
from pytest_mock import MockerFixture
from docs.macros import define_env
from mkdocs_macros.plugin import MacrosPlugin
from mkdocs.config.base import load_config


class MockRelease:
    def __init__(
        self, title, html_url, tag_name, published_at, draft, prerelease, body
    ):
        self.title = title
        self.html_url = html_url
        self.tag_name = tag_name
        self.published_at = published_at
        self.draft = draft
        self.prerelease = prerelease
        self.body = body


class MockRepository:
    def get_releases(self):
        return [
            MockRelease("title1", "url1", "tag1", "date1", False, False, "body1"),
            MockRelease("title2", "url2", "tag2", "date2", False, False, "body2"),
        ]


class MockGithub:
    def get_repo(self, owner, repo=None):
        return MockRepository()

    def render_markdown(self, text, context=None):
        return text


def test_github_releases(mocker: MockerFixture):
    os.environ["GITHUB_JOB"] = "job_deploy_mkdocs_github_pages"
    mocker.patch("docs.macros.Github", return_value=MockGithub())
    config = load_config()
    env = MacrosPlugin()
    env.load_config(config.__dict__)
    env.on_config(config)
    define_env(env)
    result = env.macros["github_releases"]("owner", "repo")

    assert (
        result
        == "----\r\n##[title1](url1)\r\n:octicons-tag-24:[tag1](url1) :octicons-calendar-24: Published date1 \r\nbody1\r\n----\r\n##[title2](url2)\r\n:octicons-tag-24:[tag2](url2) :octicons-calendar-24: Published date2 \r\nbody2\r\n----\r\n"
    )
