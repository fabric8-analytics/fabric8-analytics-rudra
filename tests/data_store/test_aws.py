#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pathlib import Path
from rudra.data_store.aws import AmazonS3
from rudra.data_store.aws import NotFoundAccessKeySecret
from moto import mock_s3
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
def upload_file(request, s3):
    file_name = 'test.json'

    data_path = Path(__file__).resolve().parents[1]
    file_path = data_path.joinpath("data", file_name).absolute()

    assert file_path.is_file()

    s3.upload_file(str(file_path), 'test.json')

    return file_name


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
        assert s3.is_connected()

    @pytest.mark.usefixtures('s3')
    def test_get_name(self, s3):
        assert s3.get_name() == 'S3:{}'.format(BUCKET)

    @pytest.mark.usefixtures('s3')
    @pytest.mark.usefixtures('upload_file')
    def test_object_exists(self, s3, upload_file):
        assert s3.object_exists(upload_file)

    @pytest.mark.usefixtures('s3')
    def test_upload_file(self, s3):
        data_path = Path(__file__).resolve().parents[1]
        file_path = data_path.joinpath("data", "test.json").absolute()
        assert file_path.is_file()
        s3.upload_file(str(file_path), 'test.json')
        assert len(s3.list_bucket_keys()) > 0

    @pytest.mark.usefixtures('s3')
    def test_write_json_file(self, s3):
        s3.write_json_file('dummy.json', {"keyA": "valueB"})
        assert len(s3.list_bucket_keys()) > 0
        json_data = s3.read_json_file('dummy.json')
        assert json_data.get("keyA") == "valueB"

    @pytest.mark.usefixtures('s3')
    @pytest.mark.usefixtures('upload_file')
    def test_read_generic_file(self, s3, upload_file):
        assert len(s3.list_bucket_keys()) > 0
        content = s3.read_generic_file(upload_file)
        assert isinstance(content, (bytes, bytearray))
        assert len(content) > 0

    @pytest.mark.usefixtures('s3')
    @pytest.mark.usefixtures('upload_file')
    def test_list_bucket_objects(self, s3, upload_file):
        objects = s3.list_bucket_objects()
        assert objects
        assert list(objects)[0].key == upload_file

    @pytest.mark.usefixtures('s3')
    @pytest.mark.usefixtures('upload_file')
    def test_list_bucket_keys(self, s3, upload_file):
        bucket_keys = s3.list_bucket_keys()
        assert len(bucket_keys) > 0
        assert bucket_keys[0] == upload_file

    @pytest.mark.usefixtures('s3')
    @pytest.mark.usefixtures('upload_file')
    def test_s3_delete_object(self, s3, upload_file):
        bucket_keys = s3.list_bucket_keys()
        assert bucket_keys[0] == upload_file
        response = s3.s3_delete_object('test.json')
        assert 'Deleted' in response
        assert response.get('ResponseMetadata').get('HTTPStatusCode') == 200

    @pytest.mark.usefixtures('s3')
    @pytest.mark.usefixtures('upload_file')
    def test_s3_delete_objects(self, s3, upload_file):
        s3.write_json_file('dummy.json', {"keyA": "valueB"})
        files = [upload_file, 'dummy.json']
        assert len(s3.list_bucket_keys()) > 0
        response = s3.s3_delete_objects(files)
        assert 'Deleted' in response
        assert not {k.get('Key') for k in response.get('Deleted')} - set(files)
        assert response.get('ResponseMetadata').get('HTTPStatusCode') == 200

    @pytest.mark.usefixtures('s3')
    @pytest.mark.usefixtures('upload_file')
    def test_s3_clean_bucket(self, s3, upload_file):
        assert len(s3.list_bucket_keys()) > 0
        s3.s3_clean_bucket()
        assert len(s3.list_bucket_keys()) == 0

    @pytest.mark.usefixtures('s3')
    def test_load_matlab_multi_matrix(self, s3):

        file_name = 'matrices.mat'

        data_path = Path(__file__).resolve().parents[1]
        file_path = data_path.joinpath("data", file_name).absolute()

        assert file_path.is_file()
        s3.upload_file(str(file_path), 'mat_file/matrices.mat')
        assert len(s3.list_bucket_keys()) > 0
        content = s3.load_matlab_multi_matrix('mat_file/matrices.mat')
        assert isinstance(content, dict)

    @pytest.mark.usefixtures('s3')
    def test_s3_upload_folder(self, s3):
        dir_name = 'test_dir'
        data_path = Path(__file__).resolve().parents[1]
        dir_path = data_path.joinpath("data", dir_name).absolute()
        assert dir_path.is_dir()
        s3.s3_upload_folder(dir_path)
        assert len(s3.list_bucket_keys()) > 0

    @pytest.mark.usefixtures('s3')
    def test_store_blob(self, s3):
        s3.store_blob(json.dumps({'keyA': 'valueB'}), 'example.json')
        bucket_keys = s3.list_bucket_keys()
        assert len(bucket_keys) > 0
        assert bucket_keys[0] == 'example.json'
