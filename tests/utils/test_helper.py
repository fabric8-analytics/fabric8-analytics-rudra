import rudra.utils.helper as helper
import requests
import pytest


def test_get_github_repo_info():
    gh_repo1 = 'https://github.com/fabric8-analytics/f8a-hpf-insights'
    gh_repo2 = 'https://github.com/fabric8-analytics/f8a-hpf-insights.git'
    gh_repo3 = 'git+https://github.com/fabric8-analytics/f8a-hpf-insights'
    gh_repo4 = 'fabric8-analytics/f8a-hpf-insights'
    user, repo = helper.get_github_repo_info(gh_repo1)
    assert user == 'fabric8-analytics' and repo == 'f8a-hpf-insights'
    user, repo = helper.get_github_repo_info(gh_repo2)
    assert user == 'fabric8-analytics' and repo == 'f8a-hpf-insights'
    user, repo = helper.get_github_repo_info(gh_repo3)
    assert user == 'fabric8-analytics' and repo == 'f8a-hpf-insights'
    user, repo = helper.get_github_repo_info(gh_repo4)
    assert user == 'fabric8-analytics' and repo == 'f8a-hpf-insights'


def test_get_training_file_url():
    user = 'fabric8-analytics'
    repo = 'f8a-hpf-insights'
    file_url = helper.get_training_file_url(user, repo)
    resp = requests.get(file_url)
    assert resp.status_code == 200

    file_url = helper.get_training_file_url(user, repo, branch='training-code')
    resp = requests.get(file_url)
    assert resp.status_code == 200

    file_url = helper.get_training_file_url(
        user, repo, training_file_path='src/flask_endpoint.py')
    resp = requests.get(file_url)
    assert resp.status_code == 200


def test_load_hyper_params():
    # mock command line args
    helper.argv = ['helper.py', '{"a": 111, "b": "some text"}']
    hyper_params = helper.load_hyper_params()
    assert hyper_params.get('a') == 111
    assert hyper_params.get('b') == "some text"


def test_cache_dict_with_zero_max_size():
    cache_dict = helper.CacheDict(0)
    with pytest.raises(KeyError):
        cache_dict['key1'] = 'value1'
    assert len(cache_dict) == 0


def test_cache_dict_with_one_max_size():
    cache_dict = helper.CacheDict(1)
    cache_dict['key1'] = 'value1'
    cache_dict['key2'] = 'value2'
    assert len(cache_dict) == 1
    assert 'key2' in cache_dict
    assert 'key1' not in cache_dict


def test_cache_dict():
    # default max_len = 1024
    cache_dict = helper.CacheDict()
    for i in range(2000):
        cache_dict[i] = i * i
    assert len(cache_dict) == cache_dict.max_len
    assert cache_dict[i] == i * i
    del cache_dict[i]
    assert len(cache_dict) == cache_dict.max_len - 1
    assert cache_dict[cache_dict.max_len - 2] == pow(cache_dict.max_len - 2, 2)
    assert len(list(cache_dict)) == cache_dict.max_len - 1
    assert str(cache_dict.max_len - 2) in str(cache_dict)
