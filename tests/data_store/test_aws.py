#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pathlib import Path
from rudra.data_store.aws import AmazonS3, AmazonEmr
from rudra.data_store.aws import NotFoundAccessKeySecret
from moto import mock_s3, mock_emr
import boto3
import json
import pytest

BUCKET = 'tmp_bucket'
AWS_KEY = 'fake_key'
AWS_SECRET = 'fake_secret'


@pytest.fixture(autouse=True)
def s3_bucket():
    with mock_s3():
        boto3.client('s3').create_bucket(Bucket=BUCKET)
        yield boto3.resource('s3').Bucket(BUCKET)


@pytest.fixture
def s3(request):

    s3 = AmazonS3(aws_access_key_id=AWS_KEY,
                  aws_secret_access_key=AWS_SECRET,
                  bucket_name=BUCKET)
    s3.connect()
    assert s3.is_connected()

    def teardown():
        s3.disconnect()

    request.addfinalizer(teardown)
    return s3


@pytest.fixture
@pytest.mark.usefixtures('s3')
def upload_dir(request, s3):
    dir_path = Path(__file__).resolve().parents[1]
    test_dir_path = dir_path.joinpath("data").absolute()
    assert test_dir_path.is_dir()
    s3.s3_upload_folder(test_dir_path)


class TestAmazonS3:

    def test_connect_without_creds(self):
        with pytest.raises(NotFoundAccessKeySecret):
            s3 = AmazonS3(bucket_name=BUCKET)
            s3.connect()

    def test_connect_with_creds(self):
        s3 = AmazonS3(aws_access_key_id=AWS_KEY,
                      aws_secret_access_key=AWS_SECRET,
                      bucket_name=BUCKET)
        s3.connect()
        assert s3.is_connected()

    def test_disconnect_with_creds(self):
        s3 = AmazonS3(aws_access_key_id=AWS_KEY,
                      aws_secret_access_key=AWS_SECRET,
                      bucket_name=BUCKET)
        s3.connect()
        s3.disconnect()
        assert not s3.is_connected()

    def test_get_name(self, s3):
        assert s3.get_name() == 'S3:{}'.format(BUCKET)

    def test_object_exists(self, s3, upload_dir):
        assert s3.object_exists('data/test.json')

    def test_upload_file(self, s3):
        data_path = Path(__file__).resolve().parents[1]
        file_path = data_path.joinpath("data", "test.json").absolute()
        assert file_path.is_file()
        s3.upload_file(str(file_path), 'test.json')
        assert len(s3.list_bucket_keys()) > 0

    def test_write_json_file(self, s3):
        s3.write_json_file('dummy.json', {"keyA": "valueB"})
        assert len(s3.list_bucket_keys()) > 0
        json_data = s3.read_json_file('dummy.json')
        assert json_data.get("keyA") == "valueB"

    def test_read_generic_file(self, s3, upload_dir):
        content = s3.read_generic_file('data/test.json')
        assert isinstance(content, (bytes, bytearray))
        assert len(content) > 0

    def test_list_bucket_objects(self, s3, upload_dir):
        objects = s3.list_bucket_objects()
        assert objects
        assert len(list(objects)) > 0

    def test_list_bucket_keys(self, s3, upload_dir):
        bucket_keys = s3.list_bucket_keys()
        assert len(bucket_keys) > 0
        assert 'data/test.json' in bucket_keys

    def test_s3_delete_object(self, s3, upload_dir):
        bucket_keys = s3.list_bucket_keys()
        assert 'data/test.json' in bucket_keys
        response = s3.s3_delete_object('data/test.json')
        assert 'Deleted' in response
        assert response.get('ResponseMetadata').get('HTTPStatusCode') == 200

    def test_s3_delete_objects(self, s3, upload_dir):
        s3.write_json_file('dummy.json', {"keyA": "valueB"})
        files = ['dummy.json', 'data/test.json']
        assert len(s3.list_bucket_keys()) > 0
        response = s3.s3_delete_objects(files)
        assert 'Deleted' in response
        assert not {k.get('Key') for k in response.get('Deleted')} - set(files)
        assert response.get('ResponseMetadata').get('HTTPStatusCode') == 200

    def test_s3_clean_bucket(self, s3, upload_dir):
        assert len(s3.list_bucket_keys()) > 0
        s3.s3_clean_bucket()
        assert len(s3.list_bucket_keys()) == 0

    def test_load_matlab_multi_matrix(self, s3, upload_dir):

        assert len(s3.list_bucket_keys()) > 0
        content = s3.load_matlab_multi_matrix('data/matrices.mat')
        assert isinstance(content, dict)

    def test_s3_upload_folder(self, s3):
        dir_name = 'test_dir'
        data_path = Path(__file__).resolve().parents[1]
        dir_path = data_path.joinpath("data", dir_name).absolute()
        assert dir_path.is_dir()
        s3.s3_upload_folder(dir_path)
        assert len(s3.list_bucket_keys()) > 0

    def test_store_blob(self, s3):
        s3.store_blob(json.dumps({'keyA': 'valueB'}), 'example.json')
        bucket_keys = s3.list_bucket_keys()
        assert len(bucket_keys) > 0
        assert bucket_keys[0] == 'example.json'

    def test_read_yaml_file(self, s3, upload_dir):
        response = s3.read_yaml_file('data/test.yaml')
        assert response
        assert len(response) > 0
        assert response.get('sample') == 'file'
        assert len(response['items']) > 0
        assert response.get('items')[0]['name'] == 'item1'

    def test_read_pickle_file(self, s3, upload_dir):
        response = s3.read_pickle_file('data/test.pkl')
        assert response
        assert response.get('key1') == 'value1'
        assert len(response['key2']) > 0


@pytest.fixture
@mock_emr
def emr(request):

    emr = AmazonEmr(aws_access_key_id=AWS_KEY,
                    aws_secret_access_key=AWS_SECRET)
    emr.connect()
    assert emr.is_connected()

    def teardown():
        emr.disconnect()

    request.addfinalizer(teardown)
    return emr


class TestAmazonEMR:
    def test_connect_without_creds(self):
        with pytest.raises(NotFoundAccessKeySecret):
            emr = AmazonEmr()
            emr.connect()

    def test_connect_with_creds(self):
        emr = AmazonEmr(aws_access_key_id=AWS_KEY,
                        aws_secret_access_key=AWS_SECRET)
        emr.connect()
        assert emr.is_connected()

    def test_disconnect_with_creds(self):
        emr = AmazonEmr(aws_access_key_id=AWS_KEY,
                        aws_secret_access_key=AWS_SECRET,
                        bucket_name=BUCKET)
        emr.connect()
        emr.disconnect()
        assert not emr.is_connected()

    def test_run_flow(self, emr):
        run_job_flow_args = dict(
            Instances={
                'InstanceCount': 1,
                'KeepJobFlowAliveWhenNoSteps': True,
                'MasterInstanceType': 'c3.medium',
                'Placement': {'AvailabilityZone': 'us-east-1a'},
                'SlaveInstanceType': 'c3.xlarge',
            },
            JobFlowRole='EMR_EC2_DefaultRole',
            LogUri='s3://fakebucket/log',
            Name='HPF_training',
            ServiceRole='EMR_DefaultRole',
            VisibleToAllUsers=True)

        response = emr.run_flow(run_job_flow_args)
        status_code = response.get(
            'ResponseMetadata', {}).get('HTTPStatusCode')
        assert status_code == 200
        job_flow_id = response.get('JobFlowId')
        assert job_flow_id
        cluster_info = emr._emr.describe_cluster(ClusterId=job_flow_id)
        assert cluster_info.get("Cluster", {}).get("Name") == "HPF_training"
