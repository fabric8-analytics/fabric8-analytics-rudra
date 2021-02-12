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
from github import Github as gh


class Github:
    """Class to perform action related to github like get content / raise PR etc."""

    def __init__(self, github_token: str, repo_path: str, master_ref: str = 'master'):
        """Get github token to initialize the class."""
        self._github = gh(github_token)
        self._master_ref = master_ref
        self._repo = self._github.get_repo(repo_path)

    def get_latest_commit_hash(self) -> str:
        """Get the latest commit hash of the repo."""
        commits = self._repo.get_commits()

        # Read the latest commit of the repo at index '0'
        latest_commit_hash = commits[0].sha

        return latest_commit_hash

    def create_branch(self, branch_name: str, commit_hash: str = None) -> str:
        """Create a branch with latest code."""
        if not commit_hash:
            commit_hash = self.get_latest_commit_hash()
        ref = self._repo.create_git_ref(f'refs/heads/{branch_name}', commit_hash)
        return ref.ref

    def get_content(self, file_path: str, commit_hash: str = None) -> (str, str):
        """Read and returns content of the given file path within repo."""
        if not commit_hash:
            commit_hash = self.get_latest_commit_hash()
        contents = self._repo.get_contents(file_path, ref=commit_hash)
        return contents.sha, contents.decoded_content.decode('utf8')

    def update_content(self, branch_name: str, file_path: str, update_message: str,
                       current_sha: str, file_content: str) -> str:
        """Update the file content for path."""
        update = self._repo.update_file(file_path, update_message, file_content,
                                        current_sha, branch=f'refs/heads/{branch_name}')
        return update['commit'].sha

    def create_pr(self, branch_name: str, title: str, body: str) -> str:
        """Raise the PR to merge changes from branch to master."""
        pr = self._repo.create_pull(title=title, body=body, head=f'refs/heads/{branch_name}',
                                    base=f'refs/heads/{self._master_ref}')
        return pr.number
