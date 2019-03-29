"""Pypi bigquery implementation."""
import time
import os
from collections import Counter

from rudra.data_store.bigquery.base import BigqueryBuilder
from rudra.utils.pypi_parser import pip_req
from rudra.data_store.bigquery.base import DataProcessing
from rudra import logger


class PyPiBigQuery(BigqueryBuilder):
    """PyPiBigQuery Implementation."""

    def __init__(self, *args, **kwargs):
        """Initialize PyPiBigQuery object."""
        super().__init__(*args, **kwargs)
        self.query_job_config.use_legacy_sql = False
        self.query_job_config.use_query_cache = True
        self.query_job_config.timeout_ms = 60000

        self.query = """
            SELECT con.content AS content
            FROM `bigquery-public-data.github_repos.contents` AS con
            INNER JOIN (SELECT files.id AS idq
                        FROM `bigquery-public-data.github_repos.languages` AS langs
                        INNER JOIN `bigquery-public-data.github_repos.files` AS files
                        ON files.repo_name = langs.repo_name
                            WHERE REGEXP_CONTAINS(TO_JSON_STRING(language), r'(?i)python')
                            AND files.path LIKE '%requirements.txt'
                    ) AS L
            ON con.id = L.id;
        """


class PyPiBigQueryDataProcessing(DataProcessing):
    """Implementation data processing for pypi bigquery."""

    def __init__(self, big_query_instance=None, s3_client=None):
        """Initialize the BigQueryDataProcessing object."""
        super().__init__(s3_client)
        self.big_query_instance = big_query_instance or PyPiBigQuery()
        self.big_query_content = list()
        self.counter = Counter()
        self.bucket_name = 'developer-analytics-audit-report'
        self.filename = '{}/big-query-data/collated.json'.format(
            os.getenv('DEPLOYMENT_PREFIX', 'dev'))

    def process(self):
        """Process Pypi Bigquery response data."""
        start = time.monotonic()
        logger.info("Running Bigquery for pypi synchronously")
        self.big_query_instance.run_query_sync()

        logger.info("fetching bigquery result.")
        for content in self.big_query_instance.get_result():
            self.big_query_content.append(content)
            logger.info("collected manifests: {}".format(len(self.big_query_content)))
        logger.info("Succefully retrieved data from Bigquery, time:{}".format(
            time.monotonic() - start))
        base_url_pypi = 'https://pypi.org/pypi/{pkg}/json'
        logger.info("Starting package cleaning")
        start_process_time = time.monotonic()
        for idx, obj in enumerate(self.big_query_content):
            start = time.monotonic()
            content = obj.get('content')
            self.process_queue = list()
            self.responses = list()
            if content:
                try:
                    for name in pip_req.parse_requirements(content):
                        logger.info("searching pkg:`{}` in Python Package Index \
                                Repository" .format(name))
                        self.async_fetch(base_url_pypi.format(pkg=name), others=name)
                except Exception as _exc:
                    logger.error("IGNORE: {}".format(_exc))
                    logger.error("Failed to parse content data {}".format(content))

                try:
                    while not self.is_fetch_done(lambda x: x.result().status_code):
                        # hold the process until all request finishes.
                        time.sleep(0.001)
                except Exception as _exc:
                    logger.error("IGNORE: {}".format(_exc))
                    # discard process_queue
                    self.process_queue = []
                    self.responses = []
                packages = sorted(set(self.handle_response()))
                if packages:
                    pkg_string = ', '.join(packages)
                    logger.info("PACKAGES: {}".format(pkg_string))
                    self.counter.update([pkg_string])
                logger.info("Processed content in time: {} process:{}/{}".format(
                    (time.monotonic() - start), idx, len(self.big_query_content)))
        logger.info("Processed All the manifests in time: {}".format(
            time.monotonic() - start_process_time))

        logger.info("updating file content")
        self.update_s3_bucket(data={'pypi': dict(self.counter.most_common())},
                              bucket_name=self.bucket_name,
                              filename=self.filename)

        logger.info("Succefully Processed the PyPiBigQuery")

    def handle_response(self):
        """Process and get the response of async requests."""
        results = list()
        for resp in self.responses:
            pkg_name, req_obj = resp
            if isinstance(req_obj, int):
                if req_obj == 200:
                    results.append(pkg_name)
            elif req_obj.status_code == 200:
                results.append(pkg_name)
                logger.info("Received status:{} for pkg:{}".format(req_obj.status_code, pkg_name))
            else:
                logger.info("Received status:{} for pkg:{}".format(req_obj.status_code, pkg_name))
        return results
