"""Validation Utility module."""

import urllib.request as request
from rudra import logger


def check_field_exists(input_data, fields):
    """Check field exist in the input data."""
    if isinstance(input_data, dict):
        for field in fields:
            if not input_data.get(fields):
                logger.error(
                    "Please provide the valid value for the field {}".format(field))
                raise ValueError
    if isinstance(input_data, (list, dict, set, frozenset)):
        return list(set(input_data).difference(set(fields)))
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
