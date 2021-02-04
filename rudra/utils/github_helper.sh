#!/bin/bash

# Script for opening pull requests for saas-analytics from retraining pipeline.
#
# Requirements:
# GITHUB_TOKEN environment variable needs to contain a GitHub token
# which will be used to make authenticated API calls that will open
# new pull requests in the upstream saas-analytics repo.
# See: https://help.github.com/en/articles/creating-a-personal-access-token-for-the-command-line

set -e

if [[ $@ -lt 3 ]]; then
    echo "Please provide arguments in the order file_name to modify, variable to modify and variable value"
    echo "You can also provide description details as string to be put while raising Git PRs"
    exit 1
fi

if [[ -z ${GITHUB_TOKEN} ]]; then
    echo "Please provide GitHub token in GITHUB_TOKEN environment variable. Exiting..."
    exit 1
fi

if [[ ! -z $4 ]]; then
    description=$4
fi

here="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

user_name=developer-analytics-bot
repo_name=saas-analytics
user_repo_url=https://${user_name}:${GITHUB_TOKEN}@github.com/${user_name}/${repo_name}.git

upstream_repo=openshiftio/saas-analytics
upstream_repo_url=https://github.com/${upstream_repo}.git

git_home=${here}/saas-analytics
file_name=${git_home}/bay-services/$1
branch_name=bump-$1-$3
model_version=$3

# Setting up global git user
echo "Setting up git user"
git config --global user.name ${user_name}

# Clone the user-repo, set upstream and branch out
echo "Cloning the saas-analytics user-repo..."
(git clone ${user_repo_url} ${git_home} || : ) && \
    cd ${git_home} && \
    git stash && \
    git checkout master && \
    git reset --hard origin/master && \
    git clean -f -d && \
    git remote set-url origin ${user_repo_url} && \
    (git remote add upstream ${upstream_repo_url} || : ) && \
    git pull --rebase upstream master && \
    (git checkout -b ${branch_name} || git checkout ${branch_name})

# Make the necessary retraining version related changes
echo "Modifying the contents of the file with new Version"
sed -i.bckp 's#MODEL_VERSION: .*#MODEL_VERSION: "'$model_version'"#' ${file_name}

# Add the modified files
echo "Adding the modified files"
git add ${file_name}

# Commit the changes
echo "Committing the changes"
git commit -m "bump $1 with version $3"

# Push the changes onto user Branch
echo "Pushing the changes to origin"
git push origin ${branch_name}

echo "Opening pull request for ${branch_name}"
echo $(curl -X POST -H 'Content-Type: application/json' -H "Authorization: token ${GITHUB_TOKEN}" -d "\
        { \
            \"title\": \"Bump $1 to use model version $3\", \
            \"body\": \"${description}\", \
            \"head\": \"${user_name}:${branch_name}\", \
            \"base\": \"master\" \
        } \
       " https://api.github.com/repos/${upstream_repo}/pulls)
echo "DONE"
