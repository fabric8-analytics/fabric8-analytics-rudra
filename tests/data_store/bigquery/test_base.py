import os
import uuid
import sqlite3
import types
from pathlib import Path

import mock
import pytest

from rudra.data_store.bigquery.base import DataProcessing

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/path-to-credentials'


class MockDB:
    def __init__(self, db=':memory:'):
        self.session = sqlite3.connect(db)
        self.cols = ['id', 'name', 'content']
        self._create_data()

    def _create_data(self):
        query = """
        create table manifests({id} int, {nm} char, {cn} char);
        insert into manifests ({id}, {nm}, {cn}) values (1, 'requirements.txt', '{rq}');
        insert into manifests ({id}, {nm}, {cn}) values (2, 'requirements.txt', '{rq}');
        insert into manifests ({id}, {nm}, {cn}) values (3, 'package.json', '{pk}');
        insert into manifests ({id}, {nm}, {cn}) values (4, 'package.json', '{pk}');
        insert into manifests ({id}, {nm}, {cn}) values (5, 'pom.xml', '{pm}');
        insert into manifests ({id}, {nm}, {cn}) values (6, 'pom.xml', '{pm}');
        """.format(**dict(zip(('id', 'nm', 'cn'), self.cols)),
                   rq=self.manifest_content('pypi'),
                   pm=self.manifest_content('maven'),
                   pk=self.manifest_content('npm')
                   )

        self.session.executescript(query)

    def manifest_content(self, eco):
        eco_map_manifest = {'pypi': 'requirements.txt',
                            'maven': 'pom.xml',
                            'npm': 'package.json'}

        dir_path = Path(__file__).resolve().parents[2]
        test_dir_path = dir_path.joinpath("data", eco_map_manifest.get(eco)).absolute()
        with open(test_dir_path) as f:
            return f.read()

    def run(self, query):
        res = self.session.execute(query).fetchall()
        return [dict(zip(self.cols, r)) for r in res]


class QueryJob:

    def __init__(self, qry=None, job_id=uuid.uuid4()):
        self.output = MockDB().run(qry)
        self.job_id = job_id
        self.state = 'PENDING'

    def result(self):
        return self.output

    def done(self):
        return True

    def __iter__(self):
        return iter(self.output)


class MockBigQuery(mock.Mock):

    QueryJobConfig = type('QueryJobConfig', (), {'priority': None})

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.job = self.Job

    class Client:
        def __init__(self, *args, **kwargs):
            self._state_flag = False

        def query(self, qry, *args, **kwargs):
            self.qry = qry
            return QueryJob(self.qry)

        def get_job(self, job_id):
            query_job = QueryJob(qry=self.qry, job_id=job_id)
            query_job.state = ['PENDING', 'DONE'][self._state_flag]
            self._state_flag = True
            return query_job

    class Job:
        QueryJobConfig = type('QueryJobConfig', (), {})

    class QueryPriority:
        BATCH = 'batch'


@pytest.fixture
@mock.patch('rudra.data_store.bigquery.base.bigquery', new_callable=MockBigQuery)
def _builder_client(_mock_bigquery_obj):
    from rudra.data_store.bigquery.base import BigqueryBuilder
    _client = BigqueryBuilder()
    _client.query = "select id, name, content from manifests"
    return _client


class TestBigQueryBuilder:

    def test_init(self, _builder_client):
        assert _builder_client.credential_path == os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
        assert isinstance(_builder_client.query_job_config, MockBigQuery.Job.QueryJobConfig)

    def test_run_query(self, _builder_client):
        job_id = _builder_client._run_query()
        assert job_id is not None

    def test_run_query_sync(self, _builder_client):
        job_id = _builder_client.run_query_sync()
        assert job_id is not None

    def test_run_query_async(self, _builder_client):
        job_id = _builder_client.run_query_async()
        assert job_id is not None
        assert _builder_client.get_status(job_id) == 'PENDING'
        assert _builder_client.get_status(job_id) == 'DONE'

    def test_get_result_sync(self, _builder_client):
        job_id = _builder_client.run_query_sync()
        assert job_id is not None
        result = _builder_client.get_result()
        assert isinstance(result, types.GeneratorType)
        result = list(result)
        assert len(result) > 0
        for d in result:
            assert not set(['id', 'name', 'content']).difference(d)

    def test_get_result_async(self, _builder_client):
        job_id = _builder_client.run_query_async()
        assert job_id is not None
        result = _builder_client.get_result(job_id=job_id)
        assert isinstance(result, types.GeneratorType)
        result = list(result)
        assert len(result) > 0
        for d in result:
            assert not set(['id', 'name', 'content']).difference(d)

    def test_iter_(self, _builder_client):
        job_id = _builder_client.run_query_sync()
        assert job_id is not None
        for d in _builder_client:
            assert not set(['id', 'name', 'content']).difference(d)


class TestDataProcessing:

    def test_async_fetch(self):
        dpro = DataProcessing()
        pkg = 'flask'
        url = 'https://pypi.org/pypi/{p}/json'.format(p=pkg)
        dpro.process_queue = list()
        dpro.responses = list()
        for _ in range(10):
            dpro.async_fetch(url, others='flask')
        assert len(dpro.process_queue) == 10
        assert len(dpro.responses) == 0
        while dpro.process_queue:
            _pkg, _url, _obj = dpro.process_queue[-1]
            assert _pkg == pkg
            assert _url == url
            if _obj.done():
                code = _obj.result().status_code
                assert code == 200
                dpro.process_queue.pop()

    def test_is_fetch_done(self):
        dpro = DataProcessing()
        pkg = 'flask'
        url = 'https://pypi.org/pypi/{p}/json'.format(p=pkg)
        dpro.process_queue = list()
        dpro.responses = list()
        num = 10
        for _ in range(num):
            dpro.async_fetch(url, others='flask')
        assert len(dpro.process_queue) == num
        assert len(dpro.responses) == 0
        while not dpro.is_fetch_done(lambda x: x.result().status_code):
            pq_len = len(dpro.process_queue)
            if pq_len < num:
                assert len(dpro.responses) == num - pq_len

    def test_caching(self):
        dpro = DataProcessing()
        pkg = 'flask'
        url = 'https://pypi.org/pypi/{p}/json'.format(p=pkg)
        dpro.process_queue = list()
        dpro.responses = list()
        num = 10000
        for _ in range(num):
            dpro.async_fetch(url, others='flask')
        assert len(dpro.process_queue) == num
        assert len(dpro.responses) == 0
        while not dpro.is_fetch_done(lambda x: x.result().status_code):
            pq_len = len(dpro.process_queue)
            if pq_len < num:
                assert len(dpro.responses) == num - pq_len
                assert url in dpro.cache
                assert dpro.cache[url] == (pkg, 200)
