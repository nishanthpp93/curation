# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.3'
#       jupytext_version: 0.8.3
#   kernelspec:
#     display_name: Python 2
#     language: python
#     name: python2
#   language_info:
#     codemirror_mode:
#       name: ipython
#       version: 2
#     file_extension: .py
#     mimetype: text/x-python
#     name: python
#     nbconvert_exporter: python
#     pygments_lexer: ipython2
#     version: 2.7.13
# ---

# # EHR Operations

# +
# %load_ext google.datalab.kernel

import google.datalab.bigquery as bq
import os

app_id = os.environ.get('APPLICATION_ID')
# %datalab project set -p $app_id
# -

# ## Most Recent Bucket Uploads
# _Based on `storage.objects.create` log events of objects named `person.csv` archived in BigQuery_

bq.Query('''
SELECT
  h.hpo_id AS hpo_id,
  m.Site_Name AS site_name,
  protopayload_auditlog.authenticationInfo.principalEmail AS email,
  resource.labels.bucket_name AS bucket_name,
  MAX(SUBSTR(protopayload_auditlog.resourceName, 34)) AS resource_name,
  MAX(timestamp) AS upload_timestamp
FROM
  `aou-res-curation-prod.lookup_tables.hpo_id_bucket_name` h
  JOIN `aou-res-curation-prod.lookup_tables.hpo_site_id_mappings` m ON h.hpo_id = m.HPO_ID
  LEFT JOIN `all-of-us-rdr-prod.GcsBucketLogging.cloudaudit_googleapis_com_data_access_2018*` l
   ON l.resource.labels.bucket_name = h.bucket_name
WHERE
  _TABLE_SUFFIX BETWEEN '0801' AND '1024'
  AND protopayload_auditlog.authenticationInfo.principalEmail IS NOT NULL
  AND protopayload_auditlog.authenticationInfo.principalEmail <> 'aou-res-curation-prod@appspot.gserviceaccount.com'
  AND protopayload_auditlog.methodName = 'storage.objects.create'
  AND resource.labels.bucket_name LIKE 'aou%'
  AND protopayload_auditlog.resourceName LIKE '%person.csv'
GROUP BY
  h.hpo_id,
  m.Site_Name,
  resource.labels.bucket_name,
  protopayload_auditlog.authenticationInfo.principalEmail
ORDER BY MAX(timestamp) ASC
''').execute(output_options=bq.QueryOutput.dataframe(use_cache=False)).result()

# ## EHR Site Submission Counts

bq.Query('''
SELECT table_id, row_count, l.*
FROM `aou-res-curation-prod.prod_drc_dataset.__TABLES__` AS t
JOIN `aou-res-curation-prod.lookup_tables.hpo_site_id_mappings` AS l  
  ON STARTS_WITH(table_id,lower(l.HPO_ID))=true
WHERE table_id like '%person%' AND
NOT(table_id like '%unioned_ehr_%') AND 
l.hpo_id <> ''
ORDER BY Display_Order
''').execute(output_options=bq.QueryOutput.dataframe(use_cache=False)).result()
