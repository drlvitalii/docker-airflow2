import datetime as dt
import json

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from twilio.rest import Client


class TwilioException(Exception):
    ...


DATASET_TABLE = """scrappers-310115.twillio.phones"""


def _call_api(task_id: str = '', xcom_key: str = '', **context) -> list:
    with open('/twilio_creds.json') as data_file:
        twilio = json.load(data_file)
        if not twilio:
            msg = 'credentials not found'
            raise TwilioException(msg)
    phones_list = context['ti'].xcom_pull(task_ids=task_id, key=xcom_key)
    # phones_list = ['647 830 7212', '000 830 7212', '647 830 7210', '643 830 7210', '627 830 7212', '643 839 7210']
    print(f'Strated processling {len(phones_list)} phones..')
    valid_numbers = []
    for phone_number in phones_list:
        try:
            client = Client(twilio['key'], twilio['secret'])
            res = client.lookups.v1.phone_numbers(phone_number).fetch(country_code='CA')
            valid_numbers.append(phone_number)
        except Exception as e:
            print(f'phone number {phone_number} is not valid. Trace:{str(e)}')
    if valid_numbers:
        print(f'Valid phones found {len(valid_numbers)}')
    else:
        print('No valid phones found')
    print(f'Valid phones found {valid_numbers}')
    return valid_numbers


def _get_from_bq(xcom_key: str, **context):
    hook = BigQueryHook(bigquery_conn_id='weather_gcp_connection', use_legacy_sql=False)
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(f"""SELECT phone_number FROM {DATASET_TABLE}""")
    result = cursor.fetchall()
    phones = []
    for i in result:
        phones.append(i[0])
    print('result', phones)
    context['task_instance'].xcom_push(key=xcom_key, value=phones)


def _update_into_bq(task_id: str, xcom_key: str, **context):
    valid_phones = context['ti'].xcom_pull(task_ids=task_id, key=xcom_key)
    if not valid_phones:
        print('Valid phones not found')
        return
    query_phones = ''
    for i in valid_phones:
        query_phones += "'" + i + "'" + ','
    query_phones = query_phones[0:-1]
    print(f'Valid_phones {valid_phones}')
    hook = BigQueryHook(bigquery_conn_id='weather_gcp_connection', use_legacy_sql=False)
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(f"""
            UPDATE {DATASET_TABLE} SET  is_validated = TRUE 
                where phone_number in ({query_phones}) """)
    print('Validate phones updated')


with DAG("validate_phone_number_using_twilio",
         start_date=dt.datetime(2021, 10, 4),
         schedule_interval=None
         ) as dag:
    get_from_bq = PythonOperator(
        task_id='get_from_bq',
        provide_context=True,
        python_callable=_get_from_bq,
        op_kwargs={'xcom_key': 'bq_phones'}

    )
    validate = PythonOperator(
        task_id=f'validate_with_twilio',
        python_callable=_call_api,
        op_kwargs={'task_id': 'get_from_bq',
                   'xcom_key': 'bq_phones'}
    )
    update_valid_phones = PythonOperator(
        task_id=f'update_valid_phones',
        python_callable=_update_into_bq,
        op_kwargs={'task_id': 'validate_with_twilio',
                   'xcom_key': 'return_value'}
    )
    get_from_bq >> validate >> update_valid_phones

if __name__ == '__main__':
    _call_api()
