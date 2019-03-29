"""Maven bigquery implementation."""
from collections import Counter
import os
import time

from rudra.data_store.bigquery.base import BigqueryBuilder, DataProcessing
from rudra.utils.mercator import SimpleMercator
from rudra import logger


class MavenBigQuery(BigqueryBuilder):
    """MavenBigQuery Implementation."""

    def __init__(self, *args, **kwargs):
        """Initialize MavenBigQuery object."""
        super().__init__(*args, **kwargs)
        self.query_job_config.use_legacy_sql = False
        self.query_job_config.use_query_cache = True
        self.query_job_config.timeout_ms = 60000
        self.query = """
            SELECT con.content AS content
            FROM `bigquery-public-data.github_repos.contents` AS con
            INNER JOIN (SELECT files.id AS id
                        FROM `bigquery-public-data.github_repos.languages` AS langs
                        INNER JOIN `bigquery-public-data.github_repos.files` AS files
                        ON files.repo_name = langs.repo_name
                            WHERE REGEXP_CONTAINS(TO_JSON_STRING(language), r'(?i)java')
                            AND files.path LIKE '%pom.xml'
                    ) AS L
            ON con.id = L.id;
        """


class MavenBQDateProcessing(DataProcessing):
    """Implementation data processing for maven bigquery."""

    def __init__(self, big_query_instance=None, s3_client=None):
        """Initialize the BigQueryDataProcessing object."""
        super().__init__(s3_client)
        self.big_query_instance = big_query_instance or MavenBigQuery()
        self.big_query_content = list()
        self.counter = Counter()
        self.bucket_name = 'developer-analytics-audit-report'
        self.filename = '{}/big-query-data/collated.json'.format(
            os.getenv('DEPLOYMENT_PREFIX', 'dev'))

    def process(self):
        """Process Maven Bigquery response data."""
        start = time.monotonic()
        _processed = 1
        logger.info("Running Bigquery for maven synchronously")
        self.big_query_instance.run_query_sync()
        for content in self.big_query_instance.get_result():
            logger.info("processing bigquery result. {}".format(_processed))
            packages = sorted(set(self.construct_packages(content.get('content'))))
            if packages:
                pkg_string = ', '.join(packages)
                logger.info("PACKAGES: {}".format(pkg_string))
                self.counter.update([pkg_string])
            _processed += 1
        logger.info("Processed All the manifests in time: {}".format(
            time.monotonic() - start))

        logger.info("updating file content")
        self.update_s3_bucket(data={'maven': dict(self.counter.most_common())},
                              bucket_name=self.bucket_name,
                              filename=self.filename)

        logger.info("Succefully Processed the MavenBigQuery")

    def construct_packages(self, content):
        """Construct package list."""
        result = list()
        allowed_scopes = ['compile', 'run', 'provided']

        try:
            mercator_ins = SimpleMercator(content)
            for dep in mercator_ins.get_dependencies():
                scope, aid, gid = str(dep.scope), str(dep.artifact_id), str(dep.group_id)

                if scope in allowed_scopes and aid and gid:
                    result.append('{g}:{a}'.format(g=gid.strip(), a=aid.strip()))
        except Exception as _exc:
            logger.warn("IGNORE THIS ERROR {}".format(_exc))
            logger.warn("CONTENT: {}".format(content))
        return result
