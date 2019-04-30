"""Npm bigquery implementation."""
from collections import Counter
import os
import re
import time
import demjson

from rudra.data_store.bigquery.base import BigqueryBuilder, DataProcessing
from rudra import logger


class NpmBigQuery(BigqueryBuilder):
    """NpmBigQuery Implementation."""

    def __init__(self, *args, **kwargs):
        """Initialize NpmBigQuery object."""
        super().__init__(*args, **kwargs)
        self.query_job_config.use_legacy_sql = False
        self.query_job_config.use_query_cache = True
        self.query_job_config.timeout_ms = 60000
        self.query = """
            SELECT A.repo_name as repo_name, B.content as content
            FROM `bigquery-public-data.github_repos.files` AS A
            INNER JOIN  `bigquery-public-data.github_repos.contents` as B
            ON A.id=B.id WHERE A.path like 'package.json';
        """


class NpmBQDataProcessing(DataProcessing):
    """Implementation data processing for npm bigquery."""

    def __init__(self, big_query_instance=None, s3_client=None,
                 file_name='collated.json'):
        """Initialize the BigQueryDataProcessing object."""
        super().__init__(s3_client)
        self.big_query_instance = big_query_instance or NpmBigQuery()
        self.big_query_content = list()
        self.counter = Counter()
        self.bucket_name = 'developer-analytics-audit-report'
        self.filename = '{}/big-query-data/{}'.format(
            os.getenv('DEPLOYMENT_PREFIX', 'dev'), file_name)

    def process(self):
        """Process Npm Bigquery response data."""
        start = time.monotonic()
        _processed = 1
        logger.info("Running Bigquery for npm synchronously")
        self.big_query_instance.run_query_sync()
        for content in self.big_query_instance.get_result():
            logger.info("processing bigquery result. {}".format(_processed))
            if content:
                packages = sorted(
                    set(self.construct_packages(content.get('content'))))
                if packages:
                    pkg_string = ', '.join(packages)
                    logger.info("PACKAGES: {}".format(pkg_string))
                    self.counter.update([pkg_string])
                _processed += 1
        logger.info("Processed All the manifests in time: {}".format(
            time.monotonic() - start))

        logger.info("updating file content")
        self.update_s3_bucket(data={'npm': dict(self.counter.most_common())},
                              bucket_name=self.bucket_name,
                              filename=self.filename)

        logger.info("Succefully Processed the NpmBigQuery")

    def construct_packages(self, content):
        """Construct package from content."""
        if content:
            content = content.decode() if not isinstance(content, str) else content
            dependencies = {}
            try:
                decoded_json = demjson.decode(content)
            except Exception as _exc:
                logger.error("IGNORE {}".format(str(_exc)))
                decoded_json = self.handle_corrupt_packagejson(content)
            if decoded_json and isinstance(decoded_json, dict):
                dependencies = decoded_json.get('dependencies', {})
            return list(dependencies.keys() if isinstance(dependencies, dict) else [])
        return []

    @staticmethod
    def handle_corrupt_packagejson(content):
        """Find dependencies from corrupted/invalid package.json."""
        dependencies_pattern = re.compile(
            r'dependencies[\'"](?:|.|\s+):(?:|.|\s+)\{(.*?)\}', flags=re.DOTALL)
        dependencies = list()
        try:
            match = dependencies_pattern.search(content)
            for line in match[1].splitlines():
                for dep in line.split(','):
                    dependency_pattern = (r"(?:\"|\')(?P<pkg>[^\"]*)(?:\"|\')(?=:)"
                                          r"(?:\:\s*)(?:\"|\')?(?P<ver>.*)(?:\"|\')")
                    matches = re.search(dependency_pattern,
                                        dep.strip(), re.MULTILINE | re.DOTALL)
                    if matches:
                        dependencies.append('"{}": "{}"'.format(
                            matches['pkg'], matches['ver']))

            return demjson.decode('{"dependencies": {%s}}' % ', '.join(dependencies))
        except Exception as _exc:
            logger.error("IGNORE {}".format(str(_exc)))
            return {}
