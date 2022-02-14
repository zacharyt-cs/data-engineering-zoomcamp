import os
import logging

from datetime import datetime
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq

BUCKET = os.environ.get("GCP_GCS_BUCKET")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))

def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    # storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    # storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

coloured_taxi_args = {
    "owner": "airflow",
    "start_date": datetime(2019, 1, 1),
    "end_date": datetime(2020, 12, 1),
    "depends_on_past": False,
    "retries": 1,
}

# define function that contains tasks
def pipeline_dag(
    #parameters
    dag,
    dataset_url,
    dataset_csv_filepath,
    dataset_parquet_filepath,
    dataset_gcspath
):
    with dag:
        download_dataset_task = BashOperator(
            task_id="download_dataset_task",
            # -l flag
            # If the server reports that the requested page  has  moved  to  a  different  location
            # this option will make curl redo the request on the new place.
            # -f flag
            # fail when HTTP request fails
            bash_command=f"curl -sSLf {dataset_url} > {dataset_csv_filepath}"
        )

        format_to_parquet_task = PythonOperator(
            task_id="format_to_parquet_task",
            python_callable=format_to_parquet,
            op_kwargs={
                "src_file": f"{dataset_csv_filepath}",
            },
        )

        local_to_gcs_task = PythonOperator(
            task_id="local_to_gcs_task",
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": BUCKET,
                "object_name": f"{dataset_gcspath}",
                "local_file": f"{dataset_parquet_filepath}",
            },
        )
        clear_file_from_cache_task = BashOperator(
            task_id="clear_file_from_cache_task",
            bash_command=f'rm {dataset_csv_filepath} {dataset_parquet_filepath}'
        )

        download_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> clear_file_from_cache_task

# yellow taxi dag
URL_PREFIX = "https://s3.amazonaws.com/nyc-tlc/trip+data"

YELLOW_TAXI_URL = URL_PREFIX + '/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
YELLOW_TAXI_CSV_FILEPATH = AIRFLOW_HOME + '/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
YELLOW_TAXI_PARQUET_FILEPATH = AIRFLOW_HOME + '/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
YELLOW_TAXI_GCSPATH = "raw/yellow_tripdata/{{ execution_date.strftime(\'%Y\') }}/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"

yellow_taxi_dag = DAG(
    #parameters
    dag_id="yellowtaxi_dag_v3",
    schedule_interval="@monthly",
    default_args=coloured_taxi_args,
    catchup=True,
    max_active_runs=3,
    tags=['dtc-de']
)

pipeline_dag(
    dag=yellow_taxi_dag,
    dataset_url=YELLOW_TAXI_URL,
    dataset_csv_filepath=YELLOW_TAXI_CSV_FILEPATH,
    dataset_parquet_filepath=YELLOW_TAXI_PARQUET_FILEPATH,
    dataset_gcspath=YELLOW_TAXI_GCSPATH
)

# green taxi dag
GREEN_TAXI_URL = URL_PREFIX + '/green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
GREEN_TAXI_CSV_FILEPATH = AIRFLOW_HOME + '/green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
GREEN_TAXI_PARQUET_FILEPATH = AIRFLOW_HOME + '/green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
GREEN_TAXI_GCSPATH = "raw/green_tripdata/{{ execution_date.strftime(\'%Y\') }}/green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"

green_taxi_dag = DAG(
    #parameters
    dag_id="greentaxi_dag_v1",
    schedule_interval="@monthly",
    default_args=coloured_taxi_args,
    catchup=True,
    max_active_runs=3,
    tags=['dtc-de']
)

pipeline_dag(
    dag=green_taxi_dag,
    dataset_url=GREEN_TAXI_URL,
    dataset_csv_filepath=GREEN_TAXI_CSV_FILEPATH,
    dataset_parquet_filepath=GREEN_TAXI_PARQUET_FILEPATH,
    dataset_gcspath=GREEN_TAXI_GCSPATH
)

# fhv dag
fhv_args = {
    "owner": "airflow",
    "start_date": datetime(2019, 1, 1),
    "end_date": datetime(2019, 12, 1),
    "depends_on_past": False,
    "retries": 1
}

FHV_URL = URL_PREFIX + '/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
FHV_CSV_FILEPATH = AIRFLOW_HOME + '/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
FHV_PARQUET_FILEPATH = AIRFLOW_HOME + '/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
FHV_GCSPATH = "raw/fhv_tripdata/{{ execution_date.strftime(\'%Y\') }}/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"

fhv_dag = DAG(
    #parameters
    dag_id="fhv_dag_v2",
    schedule_interval='@monthly',
    default_args=fhv_args,
    catchup=True,
    max_active_runs=1,
    tags=['dtc-de']
)

pipeline_dag(
    dag=fhv_dag,
    dataset_url=FHV_URL,
    dataset_csv_filepath=FHV_CSV_FILEPATH,
    dataset_parquet_filepath=FHV_PARQUET_FILEPATH,
    dataset_gcspath=FHV_GCSPATH
)


