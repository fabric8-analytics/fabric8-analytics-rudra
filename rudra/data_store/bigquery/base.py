"""Implementation Bigquery builder base."""
import os
import time
import tempfile
import json
from google.cloud import bigquery

from rudra import logger
from rudra.data_store.aws import AmazonS3


_POLLING_DELAY = 1  # sec


class BigqueryBuilder:
    """BigqueryBuilder class Implementation."""

    def __init__(self, query_job_config=None):
        """Initialize the BigqueryBuilder object."""
        logger.info("Creating BigQuery Auth Credentials")
        gcp_type = os.getenv("GCP_TYPE", "")
        gcp_project_id = os.getenv("GCP_PROJECT_ID", "")
        gcp_private_key_id = os.getenv("GCP_PRIVATE_KEY_ID", "")
        gcp_private_key = os.getenv("GCP_PRIVATE_KEY", "")
        gcp_client_email = os.getenv("GCP_CLIENT_EMAIL", "")
        gcp_client_id = os.getenv("GCP_CLIENT_ID", "")
        gcp_auth_uri = os.getenv("GCP_AUTH_URI", "")
        gcp_token_uri = os.getenv("GCP_TOKEN_URI", "")
        gcp_auth_provider_cert_url = os.getenv("GCP_AUTH_PROVIDER_X509_CERT_URL", "")
        gcp_client_url = os.getenv("GCP_CLIENT_X509_CERT_URL", "")

        key_file_contents = \
            """
            {{
              "type": "{type}",
              "project_id": "{project_id}",
              "private_key_id": "{private_key_id}",
              "private_key": "{private_key}",
              "client_email": "{client_email}",
              "client_id": "{client_id}",
              "auth_uri": "{auth_uri}",
              "token_uri": "{token_uri}",
              "auth_provider_x509_cert_url": "{auth_provider_cert_url}",
              "client_x509_cert_url": "{client_url}"
            }}
            """.format(type=gcp_type,
                       project_id=gcp_project_id,
                       private_key_id=gcp_private_key_id,
                       private_key=gcp_private_key,
                       client_email=gcp_client_email,
                       client_id=gcp_client_id,
                       auth_uri=gcp_auth_uri,
                       token_uri=gcp_token_uri,
                       auth_provider_cert_url=gcp_auth_provider_cert_url,
                       client_url=gcp_client_url)
        logger.info('Storing BigQuery Auth Credentials')
        tfile = tempfile.NamedTemporaryFile(mode='w+', delete=False)
        tfile.write(key_file_contents)
        tfile.flush()
        tfile.seek(0)
        self.credential_path = tfile.name
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = self.credential_path

        if isinstance(query_job_config, bigquery.job.QueryJobConfig):
            self.query_job_config = query_job_config
        else:
            self.query_job_config = bigquery.job.QueryJobConfig()

        self.client = None

        if self.credential_path:
            self.client = bigquery.Client(
                default_query_job_config=self.query_job_config)
        else:
            raise ValueError("Please provide the the valid credential_path")

    def _run_query(self, job_config=None):
        if self.client and self.query:
            self.job_query_obj = self.client.query(
                self.query, job_config=job_config)
            while not self.job_query_obj.done():
                time.sleep(0.1)
            return self.job_query_obj.job_id
        else:
            raise ValueError

    def run_query_sync(self):
        """Run the bigquery synchronously."""
        return self._run_query()

    def run_query_async(self):
        """Run the bigquery asynchronously."""
        job_config = bigquery.QueryJobConfig()
        job_config.priority = bigquery.QueryPriority.BATCH
        return self._run_query(job_config=job_config)

    def get_status(self, job_id):
        """Get the job status of async query."""
        response = self.client.get_job(job_id)
        return response.state

    def get_result(self, job_id=None, job_query_obj=None):
        """Get the result of the job."""
        if job_id is None:
            job_query_obj = job_query_obj or self.job_query_obj
            for row in job_query_obj.result():
                yield ({k: v for k, v in row.items()})
        else:
            job_obj = self.client.get_job(job_id)
            while job_obj.state == 'PENDING':
                job_obj = self.client.get_job(job_id)
                logger.info("Job State for Job Id:{} is {}".format(
                    job_id, job_obj.state))
                time.sleep(_POLLING_DELAY)
            yield from self.get_result(job_query_obj=job_obj)

    def __iter__(self):
        """Iterate over the query result."""
        yield from self.get_result()


class DataProcessing:
    """Process the Bigquery Data."""

    def __init__(self, s3_client=None):
        """Initialize DataProcessing object."""
        self.s3_client = s3_client

    def update_s3_bucket(self, data,
                         bucket_name,
                         filename='collated.json'):
        """Upload s3 bucket."""
        if self.s3_client is None:
            # creat s3 client if not exists.
            self.s3_client = AmazonS3(
                bucket_name=bucket_name,
                aws_access_key_id=os.getenv('AWS_S3_ACCESS_KEY_ID'),
                aws_secret_access_key=os.getenv('AWS_S3_SECRET_ACCESS_KEY')
            )
        # connect after creating or with existing s3 client
        self.s3_client.connect()
        if not self.s3_client.is_connected():
            raise ValueError("Unable to connect to s3.")

        json_data = dict()

        if self.s3_client.object_exists(filename):
            logger.info("{} exists, updating it.".format(filename))
            json_data = self.s3_client.read_json_file(filename)
            if not json_data:
                raise ValueError("Unable to get the json data path:{}/{}"
                                 .format(bucket_name, filename))

        json_data.update(data)
        self.s3_client.write_json_file(filename, json_data)
        logger.info("Updated file Succefully!")
