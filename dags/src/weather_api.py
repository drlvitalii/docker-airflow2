import datetime
import json

import requests
from google.cloud import storage

storage_client = storage.Client.from_service_account_json('/tmp/sa_creds.json')

# DEBUG ONLY
WEATHER_API_TOKEN = ''
BUCKET_NAME = ''


def send_request(city_name: str, bucket_name: str, weather_api_token: str):
    url = 'http://api.openweathermap.org/data/2.5/weather?q={0}&appid={1}'
    response = requests.get(url.format(city_name, weather_api_token))
    response_data = response.json()
    unix_time = response_data.get("dt")
    dt = datetime.datetime.utcfromtimestamp(unix_time)
    month = dt.month if dt.month >= 10 else '0' + str(dt.month)
    day = dt.day if dt.day >= 10 else '0' + str(dt.day)
    file_name = f'{city_name.lower()}/{dt.year}/{month}/{day}/{dt.time().strftime("%H_%M_%S")}.json'
    create_json(response_data, file_name, bucket_name)


def create_json(json_object, filename: str, bucket_name: str):
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(filename)
    blob.upload_from_string(
        data=json.dumps(json_object),
        content_type='application/json'
    )
    print(f'file {filename} saved')


if __name__ == '__main__':
    cities = ['London', 'Budapest', 'Chicago']
    for city in cities:
        send_request(city, BUCKET_NAME, WEATHER_API_TOKEN)
