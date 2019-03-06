"""Utility helper functions."""

from rudra.utils.validation import check_url_alive
from urllib.parse import urljoin
from rudra import logger
from sys import argv
from json import loads

GITHUB_CONTENT_BASEURL = 'https://raw.githubusercontent.com'


def get_github_repo_info(repo_url):
    """Get the github repository information."""
    logger.info("Received repository for the information",
                extra={'github_url': repo_url})
    if repo_url.endswith('.git'):
        repo_url = repo_url[:-len('.git')]
    user, repo = repo_url.split('/')[-2:]
    user = user.split(':')[-1]
    return user, repo


def load_hyper_params():
    """Load the hyper parameter from the command line args."""
    if len(argv) > 1:
        input_data = argv[1:]
        try:
            if input_data:
                return loads(input_data[0])
        except Exception:
            logger.error("Unable to decode the hyper params")


def get_training_file_url(user, repo, branch='master', training_file_path='training/train.py'):
    """Get the training file from the github repo."""
    if not user and not repo:
        logger.error("Please provide the github user and repo",
                     extra={"user": user, "repo": repo})
        raise ValueError("Please provide the github user:{} and repo:{}"
                         .format(user, repo))

    file_url = urljoin(GITHUB_CONTENT_BASEURL,
                       '/'.join((user, repo, branch,
                                 training_file_path)))

    if not check_url_alive(file_url):
        logger.error("unable to reach the github training file path",
                     extra={'github_url': file_url})
        raise ValueError("Could not able to fetch training file")
    return file_url
