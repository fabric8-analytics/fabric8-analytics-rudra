"""Tests for the AmazonS3 interface."""

from rudra.data_store.aws import AmazonS3
from rudra.data_store.bigquery.base import DataProcessing
from moto import mock_s3
import boto3
import os
import pytest

BUCKET = 'tmp_bucket'
AWS_KEY = 'fake_key'
AWS_SECRET = 'fake_secret'


@pytest.fixture(autouse=True)
def s3_bucket():
    """S3 bucket mock."""
    with mock_s3():
        boto3.client('s3').create_bucket(Bucket=BUCKET)
        yield boto3.resource('s3').Bucket(BUCKET)


@pytest.fixture
def s3(request):
    """S3 interface."""
    s3 = AmazonS3(aws_access_key_id=AWS_KEY,
                  aws_secret_access_key=AWS_SECRET,
                  bucket_name=BUCKET)
    s3.connect()
    assert s3.is_connected()

    def teardown():
        s3.disconnect()

    request.addfinalizer(teardown)
    return s3


def test_update_s3_bucket_file_not_exist(s3):
    """Test the method DataProcessing.update_s3_bucket."""
    os.environ['AWS_S3_ACCESS_KEY_ID'] = AWS_KEY
    os.environ['AWS_S3_SECRET_ACCESS_KEY'] = AWS_SECRET

    filename = 'test/collated.json'
    assert not s3.object_exists(filename)
    data_process = DataProcessing()
    data_process.update_s3_bucket({"pypi": {"flask, django": 3}},
                                  bucket_name=BUCKET,
                                  filename=filename)
    assert s3.object_exists(filename)
    json_data = s3.read_json_file(filename)
    assert json_data
    assert 'pypi' in json_data


def test_update_s3_bucket_file_exist(s3):
    """Test the method DataProcessing.update_s3_bucket."""
    os.environ['AWS_S3_ACCESS_KEY_ID'] = AWS_KEY
    os.environ['AWS_S3_SECRET_ACCESS_KEY'] = AWS_SECRET

    filename = 'test/collated.json'
    assert not s3.object_exists(filename)
    s3.write_json_file(filename, {'pypi': {'flask': 10}})
    assert s3.object_exists(filename)

    data_process = DataProcessing()
    data_process.update_s3_bucket({"pypi": {"django": 3}},
                                  bucket_name=BUCKET,
                                  filename=filename)
    assert s3.object_exists(filename)
    json_data = s3.read_json_file(filename)
    assert json_data
    assert 'pypi' in json_data
    assert 'django' in json_data['pypi']
