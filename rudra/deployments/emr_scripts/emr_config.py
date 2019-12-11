"""Configurations for EMR instance."""


class EMRConfig:
    """Config class for EMR."""

    home_dir = '/home/hadoop'

    def __init__(self, name, log_uri, ecosystem, s3_bootstrap_uri, training_repo_url,
                 training_file_name='training/train.py', release_label='emr-5.10.0',
                 instance_count=1, instance_type='m3.xlarge', applications=[{'Name': 'MXNet'}],
                 visible_to_all_users=True, job_flow_role='EMR_EC2_DefaultRole',
                 service_role='EMR_DefaultRole', properties={}, hyper_params='{}'):
        """Initialize the EMRConfig object."""
        self.instances = {
            'KeepJobFlowAliveWhenNoSteps': False,
            'TerminationProtected': False,
            'InstanceGroups': []
        }

        self.steps = [
            {
                'Name': 'Setup Debugging',
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ['state-pusher-script']
                }
            },
        ]
        self.bootstrap_actions = []
        self.emr_config = None
        self.name = name
        self.log_uri = log_uri
        self.hyper_params = hyper_params
        self.release_label = release_label
        self.s3_bootstrap_uri = s3_bootstrap_uri
        self.applications = applications
        self.visible_to_all_users = visible_to_all_users
        self.job_flow_role = job_flow_role
        self.service_role = service_role
        self.instance_type = instance_type or 'm3.xlarge'
        self.instance_count = instance_count or 1
        self.instance_group_name = '{}_master_group'.format(ecosystem)
        self.training_repo_url = training_repo_url
        self.repo_dir = "{}/{}".format(self.home_dir, 'repo')
        self.training_file_name = training_file_name
        self.instance_type_properties = {
            "LC_ALL": "en_US.UTF-8",
            "LANG": "en_US.UTF-8",
            "PYTHONPATH": "{}/repo".format(self.home_dir),
            "PYTHONUNBUFFERED": "0"
        }
        self.instance_type_properties.update(properties)
        self.ecosystem = ecosystem

    def get_config(self):
        """Get the config object."""
        training_file = "{}/{}".format(self.repo_dir, self.training_file_name)

        download_training_code = [
            'git', 'clone', self.training_repo_url, self.repo_dir]

        if self.ecosystem=='npm':
            execute_training_code = ['/usr/local/bin/python3.7'.format(self.repo_dir),
                                     training_file]
        else:
            execute_training_code = ['python3.6'.format(self.repo_dir),
                                    training_file, self.hyper_params]

        step2 = {
            'Name': 'setup - copy files',
            'ActionOnFailure': 'TERMINATE_CLUSTER',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': download_training_code
            }
        }

        step3 = {
            'Name': 'Run training job',
            'ActionOnFailure': 'TERMINATE_CLUSTER',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': execute_training_code
            }
        }
        self.steps = self.steps + [step2, step3]

        bootstrap_action = {
            'Name': 'Metadata setup',
            'ScriptBootstrapAction': {
                    'Path': self.s3_bootstrap_uri
            }
        }
        self.bootstrap_actions.append(bootstrap_action)
        instance_group = {
            'Name': self.instance_group_name,
            'InstanceRole': 'MASTER',
            'InstanceType': self.instance_type,
            'InstanceCount': self.instance_count,
            'Configurations': [
                {
                    "Classification": "hadoop-env",
                    "Properties": {},
                    "Configurations": [
                            {
                                "Classification": "export",
                                "Configurations": [],
                                "Properties": self.instance_type_properties
                            }
                    ]
                }
            ]
        }
        self.instances['InstanceGroups'].append(instance_group)
        self.emr_config = {
            "Name": self.name,
            "LogUri": self.log_uri,
            "ReleaseLabel": self.release_label,
            "Instances": self.instances,
            "BootstrapActions": self.bootstrap_actions,
            "Steps": self.steps,
            "Applications": self.applications,
            "VisibleToAllUsers": self.visible_to_all_users,
            "JobFlowRole": self.job_flow_role,
            "ServiceRole": self.service_role
        }
        return self.emr_config
