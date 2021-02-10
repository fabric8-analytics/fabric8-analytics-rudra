# Copyright © 2020 Red Hat Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Author: Dharmendra G Patel <dhpatel@redhat.com>
#
"""Github utility providing functionality to read and write github resource."""
from github import Github


class GithubUtils:
    """Class to perform action related to github like get content / raise PR etc."""

    def __init__(self, github_token):
        """Get github token to initialize the class."""
        # Create github object using token
        self._github = Github(github_token)

    def set_repo(self, repo_path, master_ref='master'):
        """Setup repo and master ref details."""
        self._master_ref = master_ref

        # Get repo and set to private member
        self._repo = self._github.get_repo(repo_path)

    def get_latest_commit_hash(self):
        """Get the latest commit hash of the repo."""
        # Read all commits for a repo.
        commits = self._repo.get_commits()

        # Read the latest commit of the repo at index '0'
        latest_commit_hash = commits[0].sha

        return latest_commit_hash

    def create_branch(self, branch_name):
        """Create a branch with latest code."""
        # Create branch with latest commit
        ref = self._repo.create_git_ref('refs/heads/' + branch_name,
                                        self.get_latest_commit_hash())
        return ref.url

    def get_content(self, file_path):
        """Read and returns content of the given file path within repo."""
        # Read file content of a path relative to repo root directory.
        contents = self._repo.get_contents(file_path)
        return contents.sha, contents.decoded_content

    def update_content(self, branch_name, file_path, update_message, current_sha, file_content):
        """Update the file content for path."""
        # Up on given branch it willl update the file content
        update = self._repo.update_file(file_path, update_message, file_content, current_sha,
                                        branch="refs/heads/" + branch_name)
        return update['commit'].sha

    def create_pr(self, branch_name, title, body):
        """Raise the PR to merge changes from branch to master."""
        pr = self._repo.create_pull(title=title, body=body, head="refs/heads/" + branch_name,
                                    base="refs/heads/" + self._master_ref)
        return pr.number
