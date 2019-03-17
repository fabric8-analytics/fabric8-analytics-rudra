"""Customized requirements.txt pip parser.

Customize pip internal lib according to our requirements and improve performance
"""

import pip._internal.req.req_file as pip_req
import pip._internal.download as pip_download
from pip._vendor.distlib.util import normalize_name
from pip._internal.download import PipSession
from rudra import logger


def parse_requirements(content, session=PipSession(), *args, **kwargs):
    """Customize pip parse_requirements."""
    _content = get_file_content(content, session=session)

    lines_enum = pip_req.preprocess(_content, None)

    for line_number, line in lines_enum:
        try:
            req_iter = pip_req.process_line(line, 'requirements.txt', line_number, None,
                                            None, None, session, None,
                                            use_pep517=None, constraint=None)
            for req in req_iter:
                if req.name:
                    yield normalize_name(req.name)
        except Exception as _exc:
            logger.error('IGNORE: {} T(EXC):{} T(con):{}'
                         .format(str(_exc), type(_exc), type(content)))
            logger.error('IGNORE CONTENT: {}'.format(content))


def get_file_content(url, session=None):
    """Customize get file content."""
    if session:
        session.timeout = 10
    if pip_download._scheme_re.search(url.decode() if not isinstance(url, str) else url):
        try:
            resp = session.get(url)
            resp.raise_for_status()
            return resp.content.decode()
        except Exception as _exc:
            logger.error('IGNORE: {}'.format(str(_exc)))
            return ''
    return url


# MonkeyPatch pip._internal
# Customize pip internal lib according to our requirement
pip_req.parse_requirements = parse_requirements
pip_download.get_file_content = get_file_content
