"""Validation Utility module."""

import urllib.request as request
import xmlrpc.client as xmlrpclib
from rudra import logger
import re
_canonicalize_regex = re.compile(r"[-_.]+")


def nn(name):
    """Returns a normalized name."""
    # This is taken from PEP 503.
    return _canonicalize_regex.sub("-", name).lower()

def check_field_exists(input_data, fields):
    """Check field exist in the input data."""
    if isinstance(input_data, dict):
        for field in fields:
            if not input_data.get(field):
                logger.error(
                    "Please provide the valid value for the field {}".format(field))
    if isinstance(input_data, (list, dict, set, frozenset)):
        return list(set(fields).difference(set(input_data)))
    raise ValueError


def check_url_alive(url, accept_codes=[401]):
    """Validate github repo exist or not."""
    try:
        logger.info("checking url is alive", extra={"url": url})
        response = request.urlopen(url)
        status_code = response.getcode()
        if status_code in accept_codes or status_code // 100 in (2, 3):
            return True
    except Exception as exc:
        logger.debug("Unable to reach url", extra={"exception": str(exc)})
    return False


class BQValidation:
    """Add validation for ecosystems."""

    def __init__(self):
        """Initialize the BQValidation object."""
        pypi_org = xmlrpclib.ServerProxy('https://pypi.python.org/pypi')
        self.pypi_org_packages = {nn(p) for p in pypi_org.list_packages()}

    def validate_pypi(self, content):
        """Validate python packages.

        Attributes:
            content (:obj:`str` or [:obj:`str`] or {:obj:`str`}):
                list/set of packages or package str

        Returns:
            [:obj:`str`]: list of valid packages.

        Raises:
            ValueError: if content is not a type of :obj:`str` or :obj:`list`

        """
        if not isinstance(content, (str, list, set, frozenset)):
            raise ValueError("content type should be string or set/list of string")

        content = [content] if isinstance(content, str) else content

        return list(self.pypi_org_packages.intersection(content))
