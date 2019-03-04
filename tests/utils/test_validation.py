from rudra.utils.validation import check_field_exists, check_url_alive
import pytest


def test_check_field_exists():
    input_data = ['a', 'b', 'c']
    missing = check_field_exists(input_data, ['a', 'd'])
    assert 'd' in missing
    missing = check_field_exists(input_data, ['a', 'c'])
    assert not missing
    input_data = {'a': 1, 'b': 2, 'c': 3}
    missing = check_field_exists(input_data, ['a', 'd'])
    assert 'd' in missing
    with pytest.raises(ValueError):
        check_field_exists(111, ['a'])


def test_check_url_alive():
    url = 'https://google.com'
    assert check_url_alive(url)
    url = 'https://234j23ksadasca.com'
    assert not check_url_alive(url)
