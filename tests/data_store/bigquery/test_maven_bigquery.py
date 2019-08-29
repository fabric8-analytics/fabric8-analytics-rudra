"""Tests for Maven BigQuery module."""

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
        self.bucket_name = 'developer-analytics-audit-report'

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

    def connect(self):
        self.is_connected()


@pytest.fixture
@mock.patch('rudra.data_store.bigquery.base.bigquery', new_callable=MockBigQuery)
def _maven_bigquery_client(_mock_bigquery_obj):
    from rudra.data_store.bigquery.maven_bigquery import MavenBigQuery
    _client = MavenBigQuery()
    _client.query = "select id, name, content from manifests where name like '%requirements.txt'"
    return _client


@pytest.fixture
@mock.patch('rudra.data_store.bigquery.base.bigquery', new_callable=MockBigQuery)
def _data_process_client(_mock_bigquery_obj):
    from rudra.data_store.bigquery.maven_bigquery import MavenBigQuery, MavenBQDataProcessing
    _mvn_ins = MavenBigQuery()
    s3_client = MockS3(tempfile.mkdtemp())
    _mvn_ins.query = "select id, name, content from manifests\
            where name like '%pom.xml'"
    _client = MavenBQDataProcessing(_mvn_ins, s3_client=s3_client)
    return _client, s3_client


class TestMavenBigQuery:

    def test_run_query(self, _maven_bigquery_client):
        job_id = _maven_bigquery_client._run_query()
        assert job_id is not None

    def test_run_query_sync(self, _maven_bigquery_client):
        job_id = _maven_bigquery_client.run_query_sync()
        assert job_id is not None

    def test_run_query_async(self, _maven_bigquery_client):
        job_id = _maven_bigquery_client.run_query_async()
        assert job_id is not None
        assert _maven_bigquery_client.get_status(job_id) == 'PENDING'
        assert _maven_bigquery_client.get_status(job_id) == 'DONE'

    def test_get_result_sync(self, _maven_bigquery_client):
        job_id = _maven_bigquery_client.run_query_sync()
        assert job_id is not None
        result = _maven_bigquery_client.get_result()
        assert isinstance(result, types.GeneratorType)
        result = list(result)
        assert len(result) > 0
        for d in result:
            assert not set(['id', 'name', 'content']).difference(d)

    def test_get_result_async(self, _maven_bigquery_client):
        job_id = _maven_bigquery_client.run_query_async()
        assert job_id is not None
        result = _maven_bigquery_client.get_result(job_id=job_id)
        assert isinstance(result, types.GeneratorType)
        result = list(result)
        assert len(result) > 0
        for d in result:
            assert not set(['id', 'name', 'content']).difference(d)


class TestMavenDataProcessing:
    def test_process(self, _data_process_client):
        dp_client, s3_client = _data_process_client
        dp_client.process()
        data = s3_client.read_json_file(dp_client.filename)
        assert 'maven' in data
        assert len(data['maven']) > 0
        for k, v in data['maven'].items():
            assert 'org.apache.camel:camel-spring-boot-starter' in k
            assert 'org.springframework.boot:spring-boot-starter-web' in k
            assert v == 2

    def test_construct_packages(self, _data_process_client):
        dp_client, s3_client = _data_process_client
        content = """
            <project><dependencies>
                    <dependency>
                        <groupId>grp1.id</groupId>
                        <artifactId>art1.id</artifactId>
                    </dependency>
                    <dependency>
                        <groupId>grp2.id</groupId>
                        <artifactId>art2.id</artifactId>
                        <scope>test</scope>
                    </dependency>
                    <dependency><groupId>gid</groupId></dependency>
            </dependencies></project>
        """
        result = dp_client.construct_packages(content)
        assert len(result) == 1
        assert 'grp1.id:art1.id' in result
