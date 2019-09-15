from datetime import date
import json
from kafka_helper import connect_kafka_producer, publish_message
from production_log_helper import production_log_record_json, production_log_time_format

if __name__ == '__main__':
    topic_name = 'production_log_test'
    print('Publishing records..')
    producer = connect_kafka_producer()
    # date_seed = date.today().strftime(production_log_time_format)
    date_seed = "1900-01-01"

    for i in range(1, 10000):
        payload = production_log_record_json(date_seed, i)
        date_seed = json.loads(payload)['end']
        publish_message(producer, topic_name, '70:3', payload)
