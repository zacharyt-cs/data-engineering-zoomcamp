import os
import logging
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
import pyarrow.csv as pv
import pyarrow.parquet as pq

BUCKET = os.environ.get("GCP_GCS_BUCKET")

# https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv
dataset_file = 'taxi_zone_lookup.csv'
dataset_url = 'https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv'
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
parquet_file = dataset_file.replace('.csv', '.parquet')

def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error("Not a csv file")
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
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1
}

with DAG(
    dag_id='zones_dag_v1',
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc_de']
) as dag:

    download_dataset_task = BashOperator(
        task_id='download_dataset_task',
        bash_command=f'curl -sS {dataset_url} > {path_to_local_home}/{dataset_file}'
    )

    format_to_parquet_task = PythonOperator(
        task_id='format_to_parquet_task',
        python_callable=format_to_parquet,
        op_kwargs={
            'src_file': f'{path_to_local_home}/{dataset_file}'
        }
    )

    local_to_gcs_task = PythonOperator(
        task_id='local_to_gcs_task',
        python_callable=upload_to_gcs,
        op_kwargs={
            'bucket': BUCKET,
            'object_name': f'raw/{parquet_file}',
            'local_file': f'{path_to_local_home}/{parquet_file}'
        }
    )

    clear_file_from_cache_task = BashOperator(
        task_id='clear_file_from_cache_task',
        bash_command=f'rm {path_to_local_home}/{dataset_file} {path_to_local_home}/{parquet_file}'
    )

    download_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> clear_file_from_cache_task
