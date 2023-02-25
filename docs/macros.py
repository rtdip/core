from github import Github

def define_env(env):

    @env.macro
    def github_releases(owner, repo):
        github_client = Github()
        repo = github_client.get_repo("{}/{}".format(owner, repo))
        output = "----\r\n"
        for release in repo.get_releases():
            title_markdown = "##[{}]({})".format(release.title, release.html_url)
            
            subtitle_markdown = ":octicons-tag-24:[{}]({}) ".format(release.tag_name, release.html_url)
            subtitle_markdown += ":octicons-calendar-24: Published {} ".format(release.published_at)
            if release.draft:
                subtitle_markdown += ":octicons-file-diff-24: Draft "
            if release.prerelease:
                subtitle_markdown += ":octicons-git-branch-24: Pre-release "
            output += "{}\r\n{}\r\n{}\r\n----\r\n".format(title_markdown, subtitle_markdown, github_client.render_markdown(release.body))
        return output