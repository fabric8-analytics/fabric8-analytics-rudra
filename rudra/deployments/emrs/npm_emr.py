"""EMR script implementation for the NPM service."""
from rudra import logger
from rudra.deployments.emrs.emr_config import EMRConfig
from rudra.deployments.emrs.emr_script_builder import EMRScriptBuilder


class NpmEMR(EMRScriptBuilder):
    """NPM Emr script implementation."""

    ecosystem = 'npm'

    def run_job(self, input_dict):
        """Run the emr job."""
        self.construct_job(input_dict)
        name = '{}_{}_training_{}'.format(
            self.env, self.ecosystem, self.current_time)

        bootstrap_uri = 's3://{bucket}/bootstrap.sh'.format(
            bucket=self.bucket_name)

        log_file_name = '{}.log'.format(name)

        log_uri = 's3://{bucket}/{log_file}'.format(
            bucket='{}-automated-analytics-spark-jobs'.format(self.env),
            log_file=log_file_name)

        emr_config_obj = EMRConfig(name=name,
                                   s3_bootstrap_uri=bootstrap_uri,
                                   training_file_url=self.training_file_url,
                                   instance_type='p3.2xlarge',
                                   log_uri=log_uri,
                                   ecosystem=self.ecosystem,
                                   properties=self.properties,
                                   hyper_params=self.hyper_params)

        configs = emr_config_obj.get_config()
        status = self.aws_emr.run_flow(configs)
        logger.info("EMR job is running {}".format(status))
        status_code = status.get('ResponseMetadata', {}).get('HTTPStatusCode')
        if status_code != 200:
            logger.error("EMR Job Failed with the status code {}".format(status_code),
                         extra={"status": status})
        return status
