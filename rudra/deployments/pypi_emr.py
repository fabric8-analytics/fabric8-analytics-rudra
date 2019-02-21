"""EMR script implementation for the PYPI service."""
from rudra.deployments.abstract_emr import AbstractEMR
from rudra.utils.validation import check_field_exists
from rudra.utils.helper import get_training_file_url, get_github_repo_info
from rudra import logger
from rudra.data_store.aws import AmazonEmr
from rudra.deployments.emr_config import EMRConfig
from time import gmtime, strftime
import os
import json


class PyPiEMR(AbstractEMR):
    """PyPiEMR Script implementation."""

    ecosystem = 'pypi'

    def __init__(self):
        """Initialize the PyPiEMR instance."""
        self.current_time = strftime("%Y_%m_%d_%H_%M_%S", gmtime())

    def submit_job(self, input_dict):
        """Submit emr job."""
        required_fields = ['hyper_params', 'environment', 'data_version',
                           'bucket_name', 'aws_access_key', 'aws_secret_key',
                           'github_repo']

        missing_fields = check_field_exists(input_dict, required_fields)

        if missing_fields:
            logger.error("Missing the parameters in input_dict",
                         extra=missing_fields)
            raise ValueError("Required fields are missing in the input {}"
                             .format(missing_fields))

        user, repo = get_github_repo_info(input_dict.get('github_repo'))
        input_dict['training_file_url'] = get_training_file_url(user, repo)

        return self.run_job(input_dict)

    def run_job(self, input_dict):
        """Run the emr job."""
        env = input_dict.get('environment')
        aws_access_key = os.getenv("AWS_S3_ACCESS_KEY_ID") \
            or input_dict.get('aws_access_key')
        aws_secret_key = os.getenv("AWS_S3_SECRET_ACCESS_KEY")\
            or input_dict.get('aws_secret_key')
        bucket_name = input_dict.get('bucket_name')
        training_file_url = input_dict.get('training_file_url')
        hyper_params = json.dumps(input_dict.get('hyper_params'),
                                  separators=(',', ':'))

        name = '{}_{}_training_{}'.format(
            env, self.ecosystem, self.current_time)

        log_file_name = '{}.log'.format(name)

        log_uri = 's3://{bucket}/{log_file}'.format(
            bucket=bucket_name, log_file=log_file_name)

        bootstrap_uri = 's3://{bucket}/bootstrap.sh'.format(bucket=bucket_name)

        properties = {
            'AWS_S3_ACCESS_KEY_ID': aws_access_key,
            'AWS_S3_SECRET_ACCESS_KEY': aws_secret_key,
        }

        emr_config_obj = EMRConfig(name=name, log_uri=log_uri, s3_bootstrap_uri=bootstrap_uri,
                                   ecosystem=self.ecosystem, training_file_url=training_file_url,
                                   properties=properties, hyper_params=hyper_params)
        aws_emr = AmazonEmr(aws_access_key_id=aws_access_key,
                            aws_secret_access_key=aws_secret_key)
        aws_emr.connect()
        if not aws_emr.is_connected():
            logger.error("Unable to connect to emr instance.")
            raise ValueError
        logger.info("Successfully connected to emr instance.")
        configs = emr_config_obj.get_config()
        status = aws_emr.run_flow(configs)
        logger.info("EMR job is running {}".format(status))
        status_code = status.get('ResponseMetadata', {}).get('HTTPStatusCode')
        if status_code != 200:
            logger.error("EMR Job Failed with the status code {}".format(status_code),
                         extra={"status": status})
        return status
