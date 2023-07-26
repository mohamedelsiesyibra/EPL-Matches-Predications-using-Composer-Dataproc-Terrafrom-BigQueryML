# Importing necessary modules
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator
)
from google.cloud import bigquery
from google.cloud import storage
from google.auth import default
import requests
import json
from datetime import datetime, timedelta
import time

# Retrieve project ID from Google Cloud default credentials
_, project_id = default()

# Replace 'YOUR_API_KEY' with your actual API key from football-data.org
API_KEY = 'YOUR_KEY'
seasons = [2020, 2021, 2022, 2023]

# Function to get Premier League results and store them in GCS bucket
def get_premier_league_results():
    # Initialize a GCS client
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('wr-epl-predictions-project')  # Get the bucket

    # Fetch and save the data for each season
    for year in seasons:
        url = f'https://api.football-data.org/v4/competitions/PL/matches?season={year}'
        headers = {'X-Auth-Token': API_KEY}
        response = requests.get(url, headers=headers)

        # Raise an exception if the request was unsuccessful
        if response.status_code != 200:
            raise Exception(f"API request failed with status {response.status_code}")

        # Save the data to GCS
        blob = bucket.blob(f'stagging-data/premier_league_results_{year}.json')  # Specify the pseudo-directory in the blob name
        blob.upload_from_string(
            data=json.dumps(response.json()),
            content_type='application/json'
        )


# Function to download and upload a JAR file to GCS
def download_and_upload_jar(**kwargs):
    # Set the local and GCS paths for the JAR file
    local_path = '/tmp/spark-bigquery-with-dependencies.jar'
    gcs_bucket = 'wr-dataproc'  # Update with your GCS bucket name
    gcs_path = f'gs://{gcs_bucket}/spark-bigquery-with-dependencies.jar'

    # Download the JAR file
    os.system(f'gsutil cp gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.29.0.jar {local_path}')

    # Upload the JAR file to GCS
    os.system(f'gsutil cp {local_path} {gcs_path}')

# Airflow DAG configuration
dag = DAG(
    'run_pyspark',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2023, 7, 23),
        'email': ['YOUR@gmail.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='DAG to pull data from API, store to GCS, create Dataproc, run PySpark, stop and delete Dataproc',
    schedule_interval='@daily',
)

# Define tasks in the DAG
pull_data = PythonOperator(
    task_id='pull_data',
    python_callable=get_premier_league_results,
    dag=dag,
)

download_jar = PythonOperator(
    task_id='download_jar',
    python_callable=download_and_upload_jar,
    dag=dag,
)

# Cluster creation
create_cluster = DataprocCreateClusterOperator(
    task_id='create_cluster',
    project_id=project_id,
    cluster_name='pl-processing',
    region='us-central1',  # changed region here
    num_workers=0,
    worker_machine_type='us-central1',
    storage_bucket='wr-dataproc',  # Update with your GCS bucket name
    dag=dag,
)

# Define PySpark job
pyspark_job = {
    'reference': {
        'job_id': f'run_pyspark_{int(time.time())}'
    },
    'placement': {
        'cluster_name': 'pl-processing'
    },
    'pyspark_job': {
        'main_python_file_uri': 'gs://wr-dataproc/dataproc-scripts/processing.py',
        'jar_file_uris': ['gs://wr-dataproc/spark-bigquery-with-dependencies.jar']
    }
}

# Submit PySpark job
run_pyspark = DataprocSubmitJobOperator(
    task_id='run_pyspark',
    region='us-central1',
    project_id=project_id,
    job=pyspark_job,
    gcp_conn_id='google_cloud_default',
    dag=dag,
)

# Cluster deletion
delete_cluster = DataprocDeleteClusterOperator(
    task_id='delete_cluster',
    project_id=project_id,
    cluster_name='pl-processing',
    region='us-central1',
    trigger_rule='all_done',
    dag=dag,
)

# Function to run BigQuery SQL from GCS bucket
def run_bigquery_sql(bucket_name, blob_name):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    sql = blob.download_as_text()

    bigquery_client = bigquery.Client()
    query_job = bigquery_client.query(sql)
    query_job.result()  # Wait for the job to complete but do not fetch the results

# SQL Transformations and ML Implementation using BQML
dateset_transformation = PythonOperator(
    task_id='dateset_transformation',
    python_callable=run_bigquery_sql,
    op_kwargs={
        'bucket_name': 'wr-epl-predictions-project',
        'blob_name': 'wr-bq-temporary-bucket/dateset-transformation.sql',
    },
    dag=dag,
)

bqml_implementation = PythonOperator(
    task_id='bqml_implementation',
    python_callable=run_bigquery_sql,
    op_kwargs={
        'bucket_name': 'wr-epl-predictions-project',
        'blob_name': 'wr-bq-temporary-bucket/bqml-implementation.sql',
    },
    dag=dag,
)

# Define task dependencies
pull_data >> download_jar >> create_cluster >> run_pyspark >> delete_cluster >> dateset_transformation >> bqml_implementation
