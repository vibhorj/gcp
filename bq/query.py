from google.cloud import bigquery
bq = bigquery.Client(project = {{PROJECT_ID}}})
#import os
#sprint(os.environ['GOOGLE_APPLICATION_CREDENTIALS'])

sql = 'SELECT * FROM `api.dummy_data`'
job = bq.query(sql) # bigquery.jobs.insert
x = job.done()