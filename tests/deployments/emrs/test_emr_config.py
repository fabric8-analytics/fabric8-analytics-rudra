from rudra.deployments.emrs.emr_config import EMRConfig


class TestEMRConfig:

    def test_get_config(self):
        emr_config_obj = EMRConfig(name="dummy_name",
                                   log_uri="s3://fake-bucket/dummy.log",
                                   ecosystem="maven",
                                   s3_bootstrap_uri="s3://fake-bucket/bootstrap.sh",
                                   training_file_url="https://github.com/train.py",
                                   instance_count=3,
                                   instance_type='p2.large',
                                   properties={'key1': 'value1'},
                                   hyper_params='{"a":123}')
        emr_config = emr_config_obj.get_config()
        assert emr_config_obj.home_dir == '/home/hadoop'
        assert emr_config_obj.instance_group_name == 'maven_master_group'
        assert isinstance(emr_config, dict)
        req_config_fields = {
            "Name", "LogUri", "ReleaseLabel", "Instances",
            "BootstrapActions", "Steps", "Applications",
            "VisibleToAllUsers", "JobFlowRole", "ServiceRole"
        }
        assert not req_config_fields - set(emr_config)
        assert emr_config['Name'] == 'dummy_name'
        assert emr_config['LogUri'] == 's3://fake-bucket/dummy.log'
        assert isinstance(emr_config['Steps'], list) \
            and len(emr_config['Steps']) > 2
        bootstrap_file_path = emr_config['BootstrapActions'][0]['ScriptBootstrapAction']['Path']
        assert bootstrap_file_path == 's3://fake-bucket/bootstrap.sh'
        assert len(emr_config['Steps']) > 0

        assert any(['python3.6' in s["HadoopJarStep"]["Args"]
                    and '{"a":123}' in s["HadoopJarStep"]["Args"] for s in emr_config['Steps']])

        for grp in emr_config['Instances']['InstanceGroups']:
            assert grp.get('InstanceType') == 'p2.large'
            for conf in grp['Configurations']:
                assert any([c['Properties'].get('key1') == 'value1'
                            for c in conf['Configurations']])
