"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
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

with DAG("weather_api",
         default_args=default_args,
         schedule_interval=timedelta(1)
         ) as dag:
    t1 = BashOperator(task_id="print_date", bash_command="date", dag=dag)
    cities = ['London', 'Chicago', 'Budapest']
    time = datetime.utcnow().strftime('%Y%M%d_%H%m')
    with TaskGroup('weather_extract_tg') as main_tg:
        for city in cities:
            t2 = PythonOperator(task_id=f"weather_extract_{city}",
                                python_callable=send_request,
                                op_kwargs={
                                    'city_name': city,
                                    'bucket_name': Variable.get("bucket_name"),
                                    'weather_api_token': Variable.get("api_token")},
                                )


    t1 >> main_tg
