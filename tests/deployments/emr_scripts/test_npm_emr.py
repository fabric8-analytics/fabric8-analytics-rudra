from moto import mock_emr
import mock
from rudra.deployments.emr_scripts import NpmEMR


class MockEMRConfig(mock.Mock):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def get_config(self):
        return dict(
            Instances={
                'InstanceCount': 1,
                'KeepJobFlowAliveWhenNoSteps': True,
                'MasterInstanceType': 'c3.medium',
                'Placement': {'AvailabilityZone': 'us-east-1a'},
                'SlaveInstanceType': 'c3.xlarge',
            },
            JobFlowRole='EMR_EC2_DefaultRole',
            LogUri='s3://fake_bucket/log',
            Name='HPF_training',
            ServiceRole='EMR_DefaultRole',
            VisibleToAllUsers=True)


class TestNpmEMR:

    @mock.patch('rudra.deployments.emr_scripts.npm_emr.EMRConfig', new_callable=MockEMRConfig)
    @mock.patch.dict('os.environ', {'AWS_S3_ACCESS_KEY_ID': 'fake_id',
                                    'AWS_S3_SECRET_ACCESS_KEY': 'fake_secret'})
    @mock_emr
    def test_run_job(self, _emr_config_mocked):
        npm_emr = NpmEMR()
        input_dict = {'environment': 'dev',
                      'data_version': '2019-01-01',
                      'bucket_name': 'fake_bucket',
                      'hyper_params': {"key1": 'value1', 'key2': 'value2'},
                      'github_repo': 'fabric8-analytics/f8a-hpf-insights'}
        response = npm_emr.run_job(input_dict)
        status_code = response.get(
            'ResponseMetadata', {}).get('HTTPStatusCode')
        assert status_code == 200
        job_flow_id = response.get('JobFlowId')
        assert job_flow_id
        job_status = npm_emr.aws_emr.get_status(job_flow_id)
        assert job_status.get('State') == 'WAITING'
