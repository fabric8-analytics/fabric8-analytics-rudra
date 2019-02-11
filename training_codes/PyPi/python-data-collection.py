
from google.cloud import bigquery

job = bigquery.job.QueryJobConfig()
job.use_legacy_sql = True
client = bigquery.Client(default_query_job_config=job)


query = """
 SELECT D.id AS id,
       repo_name,
       path,
       content
FROM   (SELECT id,
               content
        FROM   [bigquery-public-data.github_repos.contents]
        GROUP  BY id,
                  content) AS D
       INNER JOIN (SELECT id,
                          C.repo_name AS repo_name,
                          path
                   FROM   (SELECT id,
                                  repo_name,
                                  path
                           FROM
                  [bigquery-public-data:github_repos.files]
                           WHERE  LOWER(path) LIKE '%requirements.txt'
                           GROUP  BY path,
                                     id,
                                     repo_name) AS C
                          INNER JOIN (SELECT repo_name,
                                             language.name
                                      FROM
                          [bigquery-public-data.github_repos.languages]
                                      WHERE  LOWER(language.name) LIKE
                                             '%python%'
                                      GROUP  BY language.name,
                                                repo_name) AS F
                                  ON C.repo_name = F.repo_name) AS E
               ON E.id = D.id  
"""

query_job = client.query(query)

content_list = []
for row in query_job:
    content = {
        "id": row.get('id'),
        "repo_name": row.get('repo_name'),
        "path": row.get("path"),
        "content": row.get('content')
    }
    content_list.append(content)

import json

with open('python-bigquery-data.json', 'w') as f:
    json.dump(content_list, f)

with open('requirements.txt', 'a') as r, open('python-bigquery-data.json', 'r')  as f:
    content = json.load(f)
    for x in content:
        if x.get('content'):
            r.write(x.get('content'))

from pip._internal.req.req_file import parse_requirements
from pip._internal.download import PipSession
import requests
import requests_cache
from datetime import timedelta
from pip._vendor.distlib.util import normalize_name

requests_cache.install_cache(expire_after=timedelta(hours=5))

manifest_json = [{
    "ecosystem": "pypi",
    "package_list": []
}]
session = PipSession()

with open("python-bigquery-data.json", "r") as f, open('error-log-pip.txt', 'a') as log:
    content = json.load(f)
    for x in content:
        if x.get('content'):
            with open("temp-requirements.txt", "w") as w:
                w.write(x.get('content'))
            req_names = []
            try:
                for p in parse_requirements("temp-requirements.txt", session=session):
                    if p.name:
                        name = normalize_name(p.name)
                        r = requests.get(url='https://pypi.org/pypi/{}/json'.format(name), headers={"Accept": "application/json"})
                        if r.status_code == 200:
                            print("Package: {} done".format(name))
                            req_names.append(name)
            except Exception as e:
                log.write(e)
            manifest_json[0].get("package_list").append(req_names)

manifest_json[0].get("package_list")

with open("manifest-list-with-pip-api-normalized.json", "w") as w:
    json.dump(manifest_json, w)


manifest_json = [{
    "ecosystem": "pypi",
    "package_list": []
}]

with open('manifest-list-with-pip-api-normalized.json', 'r') as f:
    content = json.load(f)
    manifest_set = set()
    for package_list in content[0].get('package_list'):
        if len(package_list) > 0:
            manifest_set.add(frozenset(package_list))

manifest_json[0]['package_list'] = [list(x) for x in manifest_set]

with open('manifest-list-trimmed-unique.json', 'w') as w:
    json.dump(manifest_json, w)
