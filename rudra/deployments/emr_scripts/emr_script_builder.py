"""EMR script builder implementation."""
from rudra.deployments.emr_scripts.abstract_emr import AbstractEMR
from rudra.data_store.aws import AmazonEmr
from rudra.utils.validation import check_field_exists, check_url_alive
from rudra import logger
from time import gmtime, strftime
import os
import json


class EMRScriptBuilder(AbstractEMR):
    """EMR Script implementation."""

    def __init__(self):
        """Initialize the EMRScriptBuilder instance."""
        self.current_time = strftime("%Y_%m_%d_%H_%M_%S", gmtime())

    def construct_job(self, input_dict):
        """Submit emr job."""
        required_fields = ['environment', 'data_version',
                           'bucket_name', 'github_repo']

        missing_fields = check_field_exists(input_dict, required_fields)

        if missing_fields:
            logger.error("Missing the parameters in input_dict",
                         extra={"missing_fields": missing_fields})
            raise ValueError("Required fields are missing in the input {}"
                             .format(missing_fields))

        self.env = input_dict.get('environment')
        self.data_version = input_dict.get('data_version')
        github_repo = input_dict.get('github_repo')
        if not check_url_alive(github_repo):
            logger.error("Unable to find the github_repo {}".format(github_repo))
            raise ValueError("Unable to find the github_repo {}".format(github_repo))
        self.training_repo_url = github_repo
        self.hyper_params = input_dict.get('hyper_params', '{}')
        aws_access_key = os.getenv("AWS_S3_ACCESS_KEY_ID") \
            or input_dict.get('aws_access_key')
        aws_secret_key = os.getenv("AWS_S3_SECRET_ACCESS_KEY")\
            or input_dict.get('aws_secret_key')
        self.bucket_name = input_dict.get('bucket_name')
        if self.hyper_params:
            try:
                self.hyper_params = json.dumps(input_dict.get('hyper_params'),
                                               separators=(',', ':'))
            except Exception:
                logger.error("Invalid hyper params",
                             extra={"hyper_params": input_dict.get('hyper_params')})

        self.properties = {
            'AWS_S3_ACCESS_KEY_ID': aws_access_key,
            'AWS_S3_SECRET_ACCESS_KEY': aws_secret_key,
            'AWS_S3_BUCKET_NAME': self.bucket_name,
            'MODEL_VERSION': self.data_version,
            'DEPLOYMENT_PREFIX': self.env
        }

        self.aws_emr = AmazonEmr(aws_access_key_id=aws_access_key,
                                 aws_secret_access_key=aws_secret_key)

        self.aws_emr_client = self.aws_emr.connect()

        if not self.aws_emr.is_connected():
            logger.error("Unable to connect to emr instance.")
            raise ValueError

        logger.info("Successfully connected to emr instance.")

    def run_job(self, input_dict):
        """Run the emr job."""
        raise NotImplementedError
