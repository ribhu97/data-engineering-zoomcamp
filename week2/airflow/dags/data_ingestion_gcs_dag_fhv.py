import os
import logging

from datetime import datetime
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq

PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
BUCKET = os.environ.get('GCP_GCS_BUCKET')

# dataset_file = "yellow_tripdata_2021-01.csv"
# dataset_url = f"https://s3.amazonaws.com/nyc-tlc/trip+data/{dataset_file}"
path_to_local_home = os.environ.get('AIRFLOW_HOME',"/opt/airflow")

# BIGQUERY_DATASET = os.environ.get('BIGQUERY_DATASET','trips_data_all')

def format_to_parquet(src_file):
    """
    Function to format the data to parquet format
    """
    if not src_file.endswith('.csv'):
        logging.error('Can only accept csv files, for the moment')
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))

def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: 
    ;param bucket: GCS bucket name
    ;param object_name: target path & file-name
    ;param local_file: source path & file-name
    """
    # Workaround to prevent timout for files > 6MB on slow upload speeds
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024 # 5MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024 # 5MB
    # End of workaround

    client = storage.Client()
    bucket = client.get_bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

default_args = {
    "owner": "airflow",
    "start_date": datetime(2019,1,1),
    # "end_date": datetime(2020,12,31),
    "depends_on_past": False,
    "retries": 1,
}

URL_PREFIX = 'https://s3.amazonaws.com/nyc-tlc/trip+data/'
URL_TEMPLATE = URL_PREFIX + 'fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
OUTPUT_FILE_TEMPLATE = 'fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
parquet_file = OUTPUT_FILE_TEMPLATE.replace('.csv', '.parquet')
TABLE_NAME_TEMPLATE = 'fhv_{{ execution_date.strftime(\'%Y_%m\') }}'
BIGQUERY_DATASET = os.environ.get('BIGQUERY_DATASET',TABLE_NAME_TEMPLATE)

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="data_ingest_gcs_dag_fhv",
    schedule_interval="@monthly",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sSLf {URL_TEMPLATE} > {path_to_local_home}/{OUTPUT_FILE_TEMPLATE}"
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{path_to_local_home}/{OUTPUT_FILE_TEMPLATE}",
        },
    )

    # TODO: Homework - research and try XCOM to communicate output values between 2 tasks/operators
    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{parquet_file}",
            "local_file": f"{path_to_local_home}/{parquet_file}",
        },
    )

    local_cleanup = BashOperator(
        task_id="local_cleanup",
        bash_command=f"rm {path_to_local_home}/{OUTPUT_FILE_TEMPLATE} {path_to_local_home}/{parquet_file}"
    )
    # bigquery_external_table_task = BigQueryCreateExternalTableOperator(
    #     task_id="bigquery_external_table_task",
    #     table_resource={
    #         "tableReference": {
    #             "projectId": PROJECT_ID,
    #             "datasetId": BIGQUERY_DATASET,
    #             "tableId": "external_table",
    #         },
    #         "externalDataConfiguration": {
    #             "sourceFormat": "PARQUET",
    #             "sourceUris": [f"gs://{BUCKET}/raw/{parquet_file}"],
    #         },
    #     },
    # )

    download_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> local_cleanup