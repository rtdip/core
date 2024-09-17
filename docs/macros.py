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
from github import Github


def define_env(env):
    @env.macro
    def github_releases(owner, repo):
        # due to rate limits, only get this data on release
        release_env = os.environ.get("GITHUB_JOB", "dev")
        if release_env != "job_deploy_mkdocs_github_pages":
            return "----\r\n"

        github_client = Github(
            login_or_token=os.environ.get("GITHUB_TOKEN", None), retry=0, timeout=5
        )
        repo = github_client.get_repo("{}/{}".format(owner, repo))
        output = "----\r\n"
        for release in repo.get_releases():
            title_markdown = "##[{}]({})".format(release.title, release.html_url)

            subtitle_markdown = ":octicons-tag-24:[{}]({}) ".format(
                release.tag_name, release.html_url
            )
            subtitle_markdown += ":octicons-calendar-24: Published {} ".format(
                release.published_at
            )
            if release.draft:
                subtitle_markdown += ":octicons-file-diff-24: Draft "
            if release.prerelease:
                subtitle_markdown += ":octicons-git-branch-24: Pre-release "
            output += "{}\r\n{}\r\n{}\r\n----\r\n".format(
                title_markdown,
                subtitle_markdown,
                github_client.render_markdown(release.body),
            )
        return output
