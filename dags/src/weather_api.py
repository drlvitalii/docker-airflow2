import json

import requests
from google.cloud import storage

storage_client = storage.Client.from_service_account_json('/sa_creds.json')

# DEBUG ONLY
WEATHER_API_TOKEN = 'e5729a7c93cb11fd1468daf2686a76ab'
BUCKET_NAME = 'weatehr_api_bucket1'



def send_request(city_name: str, file_name: str, bucket_name: str, weather_api_token: str):
    url = 'http://api.openweathermap.org/data/2.5/weather?q={0}&appid={1}'
    response = requests.get(url.format(city_name, weather_api_token))
    response_data = response.json()
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
        send_request(city, BUCKET_NAME, WEATHER_API_TOKEN,)
