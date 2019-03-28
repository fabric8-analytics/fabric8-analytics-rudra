import types
import tempfile
import shutil
import json
import pathlib

import pytest
import mock


from tests.data_store.bigquery.test_base import MockBigQuery
from rudra.data_store.local_data_store import LocalDataStore


class MockS3(LocalDataStore):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.is_connected = lambda: True

    def object_exists(self, fname):
        return pathlib.Path(self.src_dir).joinpath(fname).exists()

    def write_json_file(self, fname, content):
        fpath = pathlib.Path(self.src_dir).joinpath(fname)
        if not fpath.parent.exists():
            fpath.parent.mkdir(parents=True, exist_ok=True)
        with open(str(fpath.absolute()), 'w') as json_fileobj:
            return json.dump(content, json_fileobj)

    def __del__(self):
        shutil.rmtree(self.src_dir)


@pytest.fixture
@mock.patch('rudra.data_store.bigquery.base.bigquery', new_callable=MockBigQuery)
def _pypi_bigquery_client(mock_bigquery_obj):
    from rudra.data_store.bigquery.pypi_bigquery import PyPiBigQuery
    _client = PyPiBigQuery()
    _client.query = "select id, name, content from manifests where name like '%requirements.txt'"
    return _client


@pytest.fixture
@mock.patch('rudra.data_store.bigquery.base.bigquery', new_callable=MockBigQuery)
def _data_process_client(mock_bigquery_obj):
    from rudra.data_store.bigquery.pypi_bigquery import PyPiBigQuery, PyPiBigQueryDataProcessing
    _pypi_ins = PyPiBigQuery()
    s3_client = MockS3(tempfile.mkdtemp())
    _pypi_ins.query = "select id, name, content from manifests\
            where name like '%requirements.txt'"
    _client = PyPiBigQueryDataProcessing(_pypi_ins, s3_client=s3_client)
    return _client, s3_client


class TestPyPiBigQuery:

    def test_run_query(self, _pypi_bigquery_client):
        job_id = _pypi_bigquery_client._run_query()
        assert job_id is not None

    def test_run_query_sync(self, _pypi_bigquery_client):
        job_id = _pypi_bigquery_client.run_query_sync()
        assert job_id is not None

    def test_run_query_async(self, _pypi_bigquery_client):
        job_id = _pypi_bigquery_client.run_query_async()
        assert job_id is not None
        assert _pypi_bigquery_client.get_status(job_id) == 'PENDING'
        assert _pypi_bigquery_client.get_status(job_id) == 'DONE'

    def test_get_result_sync(self, _pypi_bigquery_client):
        job_id = _pypi_bigquery_client.run_query_sync()
        assert job_id is not None
        result = _pypi_bigquery_client.get_result()
        assert isinstance(result, types.GeneratorType)
        result = list(result)
        assert len(result) > 0
        for d in result:
            assert not set(['id', 'name', 'content']).difference(d)

    def test_get_result_async(self, _pypi_bigquery_client):
        job_id = _pypi_bigquery_client.run_query_async()
        assert job_id is not None
        result = _pypi_bigquery_client.get_result(job_id=job_id)
        assert isinstance(result, types.GeneratorType)
        result = list(result)
        assert len(result) > 0
        for d in result:
            assert not set(['id', 'name', 'content']).difference(d)


class TestPyPiDataProcessing:
    def test_process(self, _data_process_client):
        dp_client, s3_client = _data_process_client
        dp_client.process()
        data = s3_client.read_json_file(dp_client.filename)
        assert 'pypi' in data
        assert len(data['pypi']) > 0
        for k, v in data['pypi'].items():
            assert 'boto' in k
            assert 'chardet' in k
            assert 'flask' in k
            assert v == 2

    def test_handle_response(self, _data_process_client):
        dp_client, s3_client = _data_process_client
        dp_client.responses = [
            ('flask', 200), ('django', type('Request', (), {"status_code": 200}))]
        result = dp_client.handle_response()
        assert len(result) == 2
        assert 'flask' in result and 'django' in result
