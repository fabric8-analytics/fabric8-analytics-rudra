#!/usr/bin/env python
# -*- coding: utf-8 -*-

from rudra.data_store.local_data_store import LocalDataStore
from pathlib import Path
import pytest
import tempfile
from distutils import dir_util


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory(prefix='local_data_store') as _dir:
        yield _dir


@pytest.fixture
def local_data_store(request, tmp_dir):

    data_store = LocalDataStore(tmp_dir)

    data_path = Path(__file__).resolve().parents[1]
    test_dir_path = data_path.joinpath("data").absolute()
    dir_util.copy_tree(test_dir_path, tmp_dir)

    def teardown():
        nonlocal data_store
        del data_store

    request.addfinalizer(teardown)
    return data_store


class TestLocalDataStore:

    def test_get_name(self, tmp_dir, local_data_store):
        assert local_data_store.get_name() == "Local filesytem dir: {}".format(tmp_dir)

    def test_read_generic_file(self, tmp_dir, local_data_store):
        response = local_data_store.read_generic_file('test.json')
        assert response
        assert len(response) > 0

    def test_read_json_file(self, tmp_dir, local_data_store):
        response = local_data_store.read_json_file('test.json')
        assert response
        assert len(response) > 0
        assert response.get('key1') == 'value1'

    #  def test_load_matlab_multi_matrix(self, tmp_dir, local_data_store):
    #      response = local_data_store.load_matlab_multi_matrix('matrices.mat')
    #      assert response
    #      assert len(response) > 0
    #      assert isinstance(response, dict)

    def test_read_yaml_file(self, tmp_dir, local_data_store):
        response = local_data_store.read_yaml_file('test.yaml')
        assert response
        assert len(response) > 0
        assert response.get('sample') == 'file'
        assert len(response['items']) > 0
        assert response.get('items')[0]['name'] == 'item1'

    def test_read_pickle_file(self, tmp_dir, local_data_store):
        response = local_data_store.read_pickle_file('test.pkl')
        assert response
        assert response.get('key1') == 'value1'
        assert len(response['key2']) > 0
