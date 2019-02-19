#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Basic interface to the Amazon S3."""

import os
import uuid
import json
import boto3
import botocore
import pickle
from pathlib import Path
from rudra import logger
from scipy.io import loadmat
from ruamel.yaml import YAML
from rudra.data_store.abstract_data_store import AbstractDataStore


class NotFoundAccessKeySecret(Exception):
    """Exception for invalid AWS secret/key."""

    def __init__(self):
        """Initialize the Exception."""
        self.message = ("AWS configuration not provided correctly, "
                        "both key id and key is needed")
        super().__init__(self.message)


class AmazonS3(AbstractDataStore):
    """Basic interface to the Amazon S3."""

    _DEFAULT_REGION_NAME = 'us-east-1'
    _DEFAULT_LOCAL_ENDPOINT = 'http://127.0.0.1:9000'  # MINIO server
    _DEFAULT_ENCRYPTION = 'aws:kms'
    _DEFAULT_VERSIONED = True

    def __init__(self, aws_access_key_id=None, aws_secret_access_key=None,
                 bucket_name=None, region_name=None, use_ssl=False,
                 encryption=None, versioned=None, local_dev=False):
        """Initialize object, setup connection to the AWS S3."""
        self._s3 = None

        self.region_name = os.getenv(
            'AWS_S3_REGION') or region_name or self._DEFAULT_REGION_NAME
        self.bucket_name = bucket_name
        self._aws_access_key_id = os.getenv(
            'AWS_S3_ACCESS_KEY_ID') or aws_access_key_id
        self._aws_secret_access_key = \
            os.getenv('AWS_S3_SECRET_ACCESS_KEY') or aws_secret_access_key

        self._local_dev = local_dev
        # let boto3 decide if we don't have local development proper values
        self._endpoint_url = self._DEFAULT_LOCAL_ENDPOINT
        self._use_ssl = True
        # 'encryption' (argument) might be False - means don't encrypt
        self.encryption = self._DEFAULT_ENCRYPTION if encryption is None else encryption
        self.versioned = self._DEFAULT_VERSIONED if versioned is None else versioned

        # if we run locally, make connection properties configurable
        if self._local_dev:
            logger.info("Running S3 locally on: {}".format(self._endpoint_url))
            self._use_ssl = use_ssl
            self.encryption = False

        if self._aws_access_key_id is None or self._aws_secret_access_key is None:
            raise NotFoundAccessKeySecret

    def object_exists(self, object_key):
        """Check if the there is an object with the given key in bucket, does only HEAD request."""
        try:
            self._s3.Object(self.bucket_name, object_key).load()
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                exists = False
            else:
                raise
        else:
            exists = True
        return exists

    def connect(self):
        """Connect to the S3 database."""
        try:
            session = boto3.session.Session(aws_access_key_id=self._aws_access_key_id,
                                            aws_secret_access_key=self._aws_secret_access_key,
                                            region_name=self.region_name)
            # signature version is needed to connect to new regions which support only v4
            if self._local_dev:
                self._s3 = session.resource('s3', config=botocore.client.Config(
                    signature_version='s3v4'),
                    use_ssl=self._use_ssl, endpoint_url=self._endpoint_url)
            else:
                self._s3 = session.resource('s3', config=botocore.client.Config(
                    signature_version='s3v4'), use_ssl=self._use_ssl)
            logger.info("Conneting to the s3")
        except Exception as exc:
            logger.info(
                "An Exception occurred while establishing a AmazonS3 connection {}"
                .format(str(exc)))

    def is_connected(self):
        """Check if the connection to database has been established."""
        return self._s3 is not None

    def disconnect(self):
        """Close the connection to S3 database."""
        del self._s3
        logger.info("Disconnected AmazonS3!")
        self._s3 = None

    @staticmethod
    def _get_fake_version_id():
        """Generate fake S3 object version id."""
        return uuid.uuid4().hex + '-unknown'

    def get_name(self):
        """Get name of this object's bucket."""
        return "S3:" + self.bucket_name

    def store_blob(self, blob, object_key):
        """Store blob onto S3."""
        put_kwargs = {'Body': blob}
        if self.encryption:
            put_kwargs['ServerSideEncryption'] = self.encryption

        return self._s3.Object(self.bucket_name, object_key).put(**put_kwargs)

    def upload_file(self, src, target):
        """Upload file into S3 Bucket."""
        try:
            return self._s3.Bucket(self.bucket_name).upload_file(src, target)
        except Exception as exc:
            logger.error(
                "An Exception occurred while uploading a file \n{}".format(str(exc)))

    def s3_upload_folder(self, folder_path, prefix=''):
        """Upload(Sync) a folder to S3.

        :folder_path: The local path of the folder to upload to s3
        :prefix: The prefix to attach to the folder path in the S3 bucket
        """
        resolved_path = Path(folder_path).resolve()
        parent_dir = resolved_path.parent
        for root, _, filenames in os.walk(resolved_path):
            for filename in filenames:
                if root != '.':
                    s3_dest = os.path.join(prefix,
                                           Path(root).relative_to(parent_dir), filename)
                else:
                    s3_dest = os.path.join(prefix, filename)
                self.upload_file(os.path.join(root, filename), s3_dest)

    def read_json_file(self, filename):
        """Read JSON file from the S3 bucket."""
        try:
            utf_data = self.read_generic_file(filename)
            # python <= 3.5 requires string to load
            if isinstance(utf_data, (bytearray, bytes)):
                utf_data = utf_data.decode('utf-8')
            return json.loads(utf_data)
        except ValueError:
            logger.error("Not a valid json file provided.")
        except Exception as exc:
            logger.error(
                "An Exception occurred while retrieving a json file \n{}".format(str(exc)))

    def read_yaml_file(self, filename):
        """Read Yaml file from the S3 bucket."""
        try:
            yaml = YAML()
            yaml_content = yaml.load(self.read_generic_file(filename))
            # convet to dict
            return json.loads(json.dumps(yaml_content))
        except ValueError:
            logger.error("Not a valid yaml file provided.")
        except Exception as exc:
            logger.error(
                "An Exception occurred while retrieving a yaml file \n{}".format(str(exc)))

    def read_pickle_file(self, filename):
        """Read Pickle file from the S3 bucket."""
        try:
            pickle_content = pickle.loads(self.read_generic_file(filename))
            return pickle_content
        except ValueError:
            logger.error("Not a valid pickle file provided.")
        except Exception as exc:
            logger.error(
                "An Exception occurred while retrieving a pickle file \n{}".format(str(exc)))

    def write_json_file(self, filename, contents):
        """Write JSON file into S3 bucket."""
        # python <= 3.5 requires str
        if isinstance(contents, (bytearray, bytes)):
            contents = contents.decode('utf-8')
        return self.store_blob(json.dumps(contents), filename)

    def read_generic_file(self, filename):
        """Retrieve remote object content."""
        try:
            return self._s3.Object(self.bucket_name, filename).get()['Body'].read()
        except Exception as exc:
            logger.error(
                "An Exception occurred while retrieving an object\n {}".format(str(exc)))

    def list_bucket_objects(self):
        """List all the objects in bucket."""
        try:
            return self._s3.Bucket(self.bucket_name).objects.all()
        except Exception as exc:
            logger.error(
                "An Exception occurred while listing objects in bucket\n {}".format(str(exc)))

    def list_bucket_keys(self):
        """List all the keys in bucket."""
        try:
            return [i.key for i in self.list_bucket_objects()]
        except Exception as exc:
            logger.error(
                "An Exception occurred while listing bucket keys\n {}".format(str(exc)))

    def s3_delete_object(self, object_key):
        """Delete a object in bucket."""
        try:
            return self._s3.Bucket(self.bucket_name).delete_objects(
                Delete={"Objects": [{'Key': object_key}]}
            )
        except Exception as exc:
            logger.error(
                "An Exception occurred while deleting object\n {}".format(str(exc)))

    def s3_delete_objects(self, object_keys):
        """Delete a object in bucket."""
        try:
            if not isinstance(object_keys, list):
                raise ValueError("Expected {}, got {}".format(
                    type(list()), type(object_keys)))
            return self._s3.Bucket(self.bucket_name).delete_objects(
                Delete={"Objects": [{'Key': k} for k in object_keys]}
            )
        except Exception as exc:
            logger.error(
                "An Exception occurred while deleting objects \n {}".format(str(exc)))

    def s3_clean_bucket(self):
        """Clean the bucket."""
        try:
            all_keys = self.list_bucket_keys()
            self.s3_delete_objects(all_keys)
            logger.info(
                "`{}` bucket has been cleaned.".format(self.bucket_name))
        except Exception as exc:
            logger.error(
                "An Exception occurred while cleaning the bucket\n {}".format(str(exc)))

    def load_matlab_multi_matrix(self, s3_path):
        """Load a '.mat'file & return a dict representation.

        :s3_path: The path of the object in the S3 bucket.
        :returns: A dict containing numpy matrices against the keys of the
                  multi-matrix.
        """
        local_filename = os.path.join('/tmp', s3_path.split('/')[-1])
        self._s3.Bucket(self.bucket_name).download_file(
            s3_path, local_filename)
        model_dict = loadmat(local_filename)
        if not model_dict:
            logger.error("Unable to load the model for scoring")
        return model_dict


class AmazonEmr(AmazonS3):
    """Basic interface to the Amazon EMR."""

    def __init__(self, *args, **kwargs):
        """Initialize object, setup connection to the AWS EMR."""
        super().__init__(*args, **kwargs)
        self._emr = None

    def connect(self):
        """Connect to the emr instance."""
        try:
            session = boto3.session.Session(aws_access_key_id=self._aws_access_key_id,
                                            aws_secret_access_key=self._aws_secret_access_key,
                                            region_name=self.region_name)

            self._emr = session.client('emr', config=botocore.client.Config(
                signature_version='s3v4'), use_ssl=self._use_ssl)
            logger.info("Connecting to the emr")
        except Exception as exc:
            logger.info(
                "An Exception occurred while establishing a AmazonEMR connection {}"
                .format(str(exc)))

    def is_connected(self):
        """Check if the connection to database has been established."""
        return self._emr is not None

    def disconnect(self):
        """Close the connection to S3 database."""
        del self._emr
        logger.info("Disconnected AmazonS3!")
        self._emr = None
