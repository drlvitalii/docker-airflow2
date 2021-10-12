from datetime import datetime, timedelta

from airflow import DAG
from custom_operators.twilio_operator import TwilioOperator

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
with DAG("twilio_custom_dag",
         default_args=default_args,
         schedule_interval=timedelta(1)
         ) as dag:
    twilio_task = TwilioOperator(
        task_id="print_twilio",
        name="test_custom_twilio_ api from dag"
    )
