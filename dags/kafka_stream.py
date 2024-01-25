from datetime import datetime
import json
import uuid
import requests
from kafka import KafkaProducer
# from airflow import DAG
# from airflow.operators.python import PythonOperator

default_arg = {
    "owner": "serge",
    "start_date": datetime(2023, 9, 3, 10, 00)
}

def get_data():
    #import requests

    res = requests.get("https://randomuser.me/api/")
    res = res.json()["results"][0]
    #res = json.dumps(res, indent=3)

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
   # from kafka import KafkaProducer
   # import requests

    res = get_data()
    res = format_data(res)
    #print(json.dumps(res, indent=3))

    producer = KafkaProducer(bootstrap_servers=["localhost:9092"], max_block_ms = 5000)

    producer.send("users_created", json.dumps(res).encode("utf-8"))


# with DAG("user_automation", default_arg=default_arg, schedule_interval="@daily", catchup=False) as dag:
#     streaming_task = PythonOperator(docker 
#         task_id="stream_data_from_api",
#         python_callable=stream_data
#     )
    
stream_data()  