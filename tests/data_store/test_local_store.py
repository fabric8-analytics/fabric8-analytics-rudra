#!/usr/bin/env python
# -*- coding: utf-8 -*-

from rudra.data_store.local_data_store import LocalDataStore
from pathlib import Path
import pytest
import tempfile
import shutil


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory(prefix='local_data_store') as _dir:
        yield _dir


@pytest.fixture
def local_data_store(request, tmp_dir):
    data_store = LocalDataStore(tmp_dir)

    file_name = 'test.json'
    mat_file = 'matrices.mat'

    data_path = Path(__file__).resolve().parents[1]
    file_path = data_path.joinpath("data", file_name).absolute()
    mat_file_path = data_path.joinpath("data", mat_file).absolute()
    shutil.copy(file_path, tmp_dir)
    shutil.copy(mat_file_path, tmp_dir)

    def teardown():
        nonlocal data_store
        del data_store

    request.addfinalizer(teardown)
    return data_store


class TestLocalDataStore:

    @pytest.mark.usefixtures('tmp_dir')
    @pytest.mark.usefixtures('local_data_store')
    def test_get_name(self, tmp_dir, local_data_store):
        assert local_data_store.get_name() == "Local filesytem dir: {}".format(tmp_dir)

    @pytest.mark.usefixtures('tmp_dir')
    @pytest.mark.usefixtures('local_data_store')
    def test_read_generic_file(self, tmp_dir, local_data_store):
        response = local_data_store.read_generic_file('test.json')
        assert response
        assert len(response) > 0

    @pytest.mark.usefixtures('tmp_dir')
    @pytest.mark.usefixtures('local_data_store')
    def test_read_json_file(self, tmp_dir, local_data_store):
        response = local_data_store.read_json_file('test.json')
        assert response
        assert len(response) > 0
        assert response.get('key1') == 'value1'

    @pytest.mark.usefixtures('tmp_dir')
    @pytest.mark.usefixtures('local_data_store')
    def test_load_matlab_multi_matrix(self, tmp_dir, local_data_store):
        response = local_data_store.load_matlab_multi_matrix('matrices.mat')
        assert response
        assert len(response) > 0
        assert isinstance(response, dict)
