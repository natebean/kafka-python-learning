import json
from kafka import KafkaConsumer
from production_log_helper import add_duration
from kafka_helper import publish_message, connect_kafka_producer


if __name__ == '__main__':
    print('Running Consumer..')
    consumed_topic_name = 'production_log_test'

    consumer = KafkaConsumer(consumed_topic_name,
                         group_id='production_log_enrichment',
                         bootstrap_servers=['localhost:9092'], auto_offset_reset='earliest',
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    
    producer = connect_kafka_producer()
    for message in consumer:
        publish_message(producer, 'production_log_with_duration', '70:3', add_duration(message.value))
        print(add_duration(message.value))
    
            
    consumer.close()
    print("done consuming")