import json
from kafka import KafkaConsumer, KafkaProducer


if __name__ == '__main__':
    print('Running Consumer..')
    parsed_records = []
    parsed_topic_name = 'production_log_test'

    consumer = KafkaConsumer(parsed_topic_name,
                         group_id='production_log_consumer',
                         bootstrap_servers=['localhost:9092'], auto_offset_reset='earliest',
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    for message in consumer:
        # message value and key are raw bytes -- decode if necessary!
        # e.g., for unicode: `message.value.decode('utf-8')`
        print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                            message.offset, message.key,
                                            message.value))
            
    consumer.close()
    print("done consuming")
