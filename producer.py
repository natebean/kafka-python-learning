import json
import random
from kafka import KafkaConsumer, KafkaProducer
from datetime import date
from datetime import timedelta
from datetime import datetime


def publish_message(producer_instance, topic_name, key, value):
    try:
        key_bytes = bytes(key, encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        print('.', end=" ")
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))


def connect_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'], api_version=(0, 10))
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer


def production_log_record(date_seed, id):
    # seed = date.today()
    # date_one = seed + timedelta(days=id)
    time_format = "%Y-%m-%d"
    date_one = datetime.strptime(date_seed, time_format )
    rand_days = random.randint(1, 10)
    date_two = date_one + timedelta(days=rand_days)
    return json.dumps({
        'id': id,
        'version': 2,
        'start': date_one.strftime(time_format),
        'end': date_two.strftime(time_format)
    }, default=str)


if __name__ == '__main__':
    parsed_records = []
    parsed_topic_name = 'production_log_test'
    print('Publishing records..')
    producer = connect_kafka_producer()
    time_format = "%Y-%m-%d"
    date_seed = date.today().strftime(time_format)

    for i in range(1, 10000):
        payload = production_log_record(date_seed, i)
        date_seed = json.loads(payload)['end']
        # print(payload)
        publish_message(producer, parsed_topic_name, '70:3', payload)
