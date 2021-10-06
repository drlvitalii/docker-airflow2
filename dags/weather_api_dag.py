"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.task_group import TaskGroup

from src.weather_api import send_request

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2021, 9, 30),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}
BUCKET_NAME = Variable.get("bucket_name")

cities = ['London', 'Chicago', 'Kyiv', 'Montreal', 'Paris', 'Rome', 'Louisiana']

with DAG("weather_api",
         default_args=default_args,
         schedule_interval=timedelta(1)
         ) as dag:
    t1 = BashOperator(task_id="print_date", bash_command="date", dag=dag)
    dt = datetime.utcnow()
    file_name_format = '{0}/{1}/{2}/{3}/{4}.json'
    month = dt.month if dt.month >= 10 else '0' + str(dt.month)
    day = dt.day if dt.day >= 10 else '0' + str(dt.day)
    time = dt.time().strftime("%H_%M")

    with TaskGroup('weather_extract_to_gcs') as api_to_gcs:
        for city in cities:
            file_name = file_name_format.format(city.lower(), dt.year, month, day, time)
            t2 = PythonOperator(task_id=f"weather_extract_{city}",
                                python_callable=send_request,
                                op_kwargs={
                                    'city_name': city,
                                    'file_name': f'{city}/test_file.json',
                                    'bucket_name': BUCKET_NAME,
                                    # 'dts_loaded': dt.strftime("%Y-%M-%dT%H:%m:%S"),
                                    'weather_api_token': Variable.get("api_token")
                                })
    with TaskGroup('weather_gcs_to_bq') as gcs_to_bq:
        for city in cities:
            file_name = file_name_format.format(city.lower(), dt.year, month, day, time)
            GCSToBigQueryOperator(
                task_id=f'gcs_to_bq__{city}',
                bucket=BUCKET_NAME,
                source_objects=[f'{city}/test_file.json'],
                source_format='NEWLINE_DELIMITED_JSON',
                destination_project_dataset_table=f'weather_api.current_weather',
                create_disposition='CREATE_IF_NEEDED',
                write_disposition='WRITE_APPEND',
                bigquery_conn_id='weather_gcp_connection',
                google_cloud_storage_conn_id='weather_gcp_connection'
            )

    t1 >> api_to_gcs >> gcs_to_bq
