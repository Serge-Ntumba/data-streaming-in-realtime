# import os, sys
# sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))

# import sys
# sys.path.append('/Users/sergentumba/.local/share/virtualenvs/data-streaming-in_realtime-vPjmjdez/lib/python3.9/site-packages')

from airflow import DAG
from kafka import KafkaProducer
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
import uuid
import time
import logging
import requests



default_args = {
    "owner": "serge",
    "start_date": datetime(2023, 9, 3, 10, 00)
}

def get_data():

    res = requests.get("https://randomuser.me/api/")
    res = res.json()["results"][0]

    return res
 
def format_data(res):
    data = {}
    location = res['location']
    data['id'] = str(uuid.uuid4())
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

    return data


def stream_data():
    producer = KafkaProducer(bootstrap_servers=["broker:29092"], max_block_ms = 5000)
    curr_time = time.time()

    while True:
        if time.time() > curr_time + 60:
            break
        try:
            res = get_data()
            res = format_data(res)
            producer.send("users_created", json.dumps(res).encode("utf-8"))
        except Exception as e:
            logging.error(f"An error occured: {e}")    


with DAG("user_automation", default_args=default_args, schedule_interval="@daily", catchup=False) as dag:
    streaming_task = PythonOperator( 
        task_id="stream_data_from_api",
        python_callable=stream_data
    )