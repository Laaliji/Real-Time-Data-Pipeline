
from confluent_kafka import Producer
import requests
import json
import time


producer_config = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'randomuser_producer'
}

kafka_topic = 'user_profiles'

producer = Producer(producer_config)


def fetch_and_publish_user_profiles():
    user_count = 0
    while True:
        try:
            response = requests.get(api_url, timeout=5)

            if response.status_code == 200:
                data = response.json()['results'][0]
                message_value = json.dumps(data).encode('utf-8')
                producer.produce(topic=kafka_topic, value=message_value)

                producer.flush()

                user_count += 1
                print(f"Published user profile to Kafka topic: {kafka_topic}")
        except Exception as e:

            print(f"Request to API failed: {e}")
            time.sleep(2)


        time.sleep(2)


api_url = "https://randomuser.me/api/?results=1"


fetch_and_publish_user_profiles()