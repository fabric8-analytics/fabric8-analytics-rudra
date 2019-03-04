from rudra.deployments.emr_scripts.emr_script_builder import EMRScriptBuilder
from time import gmtime, strftime
from contextlib import suppress
from mock import patch
import re
import ast
import pytest
import requests
import json


class TestEMRScriptBuilder:

    def test_curr_time(self):
        emr_builder_obj = EMRScriptBuilder()
        assert strftime("%Y_%m_%d_%H_%M", gmtime()
                        ) in emr_builder_obj.current_time

    def test_construct_job_without_required_params(self):
        emr_builder_obj = EMRScriptBuilder()
        req_params = {'environment', 'data_version',
                      'bucket_name', 'github_repo'}
        with suppress(FileNotFoundError):
            open('/tmp/rudra.errors.log', 'w').close()
        with pytest.raises(ValueError):
            emr_builder_obj.construct_job({})
        with open('/tmp/rudra.errors.log') as logfile:
            pattern = '(\[missing_fields: (\[.*\])\])'
            m = re.search(pattern, logfile.read())
            grps = m.groups()
            assert len(grps) > 1
            assert not set(ast.literal_eval(grps[1])) - req_params

    @patch.dict('os.environ', {'AWS_S3_ACCESS_KEY_ID': 'fake_id',
                               'AWS_S3_SECRET_ACCESS_KEY': 'fake_secret'})
    def test_construct_job(self):
        emr_builder_obj = EMRScriptBuilder()
        req_params = {'environment': 'dev',
                      'data_version': '2019-01-01',
                      'bucket_name': 'fake_bucket',
                      'hyper_params': {"key1": 'value1', 'key2': 'value2'},
                      'github_repo': 'fabric8-analytics/f8a-hpf-insights'}
        emr_builder_obj.construct_job(req_params)
        with suppress(FileNotFoundError):
            open('/tmp/rudra.errors.log', 'w').close()
        with open('/tmp/rudra.errors.log') as logfile:
            pattern = '(\[missing_fields: (\[.*\])\])'
            m = re.search(pattern, logfile.read())
            assert not m
        assert emr_builder_obj.env == req_params.get('environment')
        assert emr_builder_obj.bucket_name == req_params.get('bucket_name')
        assert emr_builder_obj.hyper_params == json.dumps(req_params.get('hyper_params'),
                                                          separators=(',', ':'))
        assert requests.get(emr_builder_obj.training_file_url).status_code == 200
        assert emr_builder_obj.properties.get('AWS_S3_ACCESS_KEY_ID') == 'fake_id'
        assert emr_builder_obj.properties.get('AWS_S3_SECRET_ACCESS_KEY') == 'fake_secret'
