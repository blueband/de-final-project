import os, sys
import logging
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from src.file_ingester import download_main, get_file_name
from google.cloud import storage
from datetime import datetime
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

print(AIRFLOW_HOME)
# from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
# import pyarrow.parquet as pq
# PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

# path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
# parquet_file = dataset_file.replace('.csv', '.parquet')
# BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')



# # NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, **kwargs):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # Retrieve data from previous task via XCOM interface 
    ti = kwargs['ti']
    local_file = ti.xcom_pull(task_ids='get_raw_data_fromgdrive_task')
    object_name = get_file_name(local_file)

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
    "retries": 1,
}

# # NOTE: DAG declaration - using a Context Manager (an implicit way)
# current_month = datetime.now().month
# current_year = datetime.now().year
with DAG(
    dag_id = 'data_fromgdrive_to_dl',
    start_date = days_ago(1),
    # depends_on_past = False,
    catchup=False,
    max_active_runs=1,
    tags=['de-project'],
    default_args={
        'owner': 'Qamarudeen Muhammad',
    }, 
    schedule_interval='0 8 2 * *',
) as dag:
    get_raw_data_fromgdrive_task = PythonOperator(
        task_id='get_raw_data_fromgdrive_task', 
        python_callable=download_main,
        op_kwargs=dict(
            month='09',
            year='2020',
            ),
        provide_context=True,
        dag=dag
    )


    # TODO: Homework - research and try XCOM to communicate output values between 2 tasks/operators
    local_to_gcs_task = PythonOperator(

        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        provide_context=True,
        op_kwargs={
            "bucket": BUCKET
        },
        
    )


#     download_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> bigquery_external_table_task

get_raw_data_fromgdrive_task >> local_to_gcs_task
