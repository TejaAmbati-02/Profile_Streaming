import json
import sys
import uuid
from pathlib import Path
from datetime import datetime

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
from kafka import KafkaProducer
import time

# Add project root to path (remove this when using Docker with PYTHONPATH set)
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils.logging_utils import logger, log_function

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 4, 8)
}

@log_function
def get_data():
    response = requests.get("https://randomuser.me/api/")
    response = response.json()
    response = response['results'][0]
    
    logger.info("Data fetched successfully from API")
    return response

@log_function
def format_data(res):
    data = {}
    location = res['location']
    data['id'] = str(uuid.uuid4())  # Fixed: convert to string for JSON
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']
    logger.info("Data formatted successfully", extra={"item_id": data['id']})
    return data


@log_function
def stream_data():
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000) # local= localhost:9092, pods = broker:29092
    curr_time = time.time()

    while True:
        if time.time() > curr_time + 60: #1 minute
            break
        try:
            response = get_data()
            response = format_data(response)
            logger.info("Streaming data to Kafka", extra={"data": json.dumps(response, indent=3)})

            producer.send('users_created', json.dumps(response).encode('utf-8'))
        except Exception as e:
            logger.error(f'An error occured: {e}')
            continue


with DAG('kafka_stream', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    streaming_task = PythonOperator(
        task_id='streaming_task_from_api',
        python_callable=stream_data
    )


if __name__ == "__main__":
    stream_data()