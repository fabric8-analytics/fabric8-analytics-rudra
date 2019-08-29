"""Tests for pypi_parser module."""

from rudra.utils.pypi_parser import pip_req
from rudra.utils.pypi_parser import get_file_content
from rudra.utils.pypi_parser import parse_requirements
from pip._internal.download import PipSession
from requests import get


def test_get_file_content():
    content = 'flask'
    _content = get_file_content(content)
    assert isinstance(_content, str) and _content == content
    content = ('https://raw.githubusercontent.com/fabric8-analytics'
               '/f8a-hpf-insights/master/requirements.txt')
    _content = get_file_content(content, PipSession())
    assert get(content).text == _content
    content = 'https://invalidUrl'
    _content = get_file_content(content, PipSession())
    assert not _content


def test_parse_requirements():
    content = """
            Flask==1.0.2
            daiquiri==1.5.0
            -e git://github.com/mozilla/elasticutils.git#egg=elasticutils
    """
    result = list(parse_requirements(content))
    assert not {'flask', 'daiquiri', 'elasticutils'}.difference(result)
    content = 'https://invalidUrl'
    result = list(parse_requirements(content))
    assert not result
    content = ''
    result = list(parse_requirements(content))
    assert not result


def test_pip_req():
    content = """
            Flask==1.0.2
            daiquiri==1.5.0
            -e git://github.com/mozilla/elasticutils.git#egg=elasticutils
    """
    result = list(pip_req.parse_requirements(content))
    assert not {'flask', 'daiquiri', 'elasticutils'}.difference(result)
