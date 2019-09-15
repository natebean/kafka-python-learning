from datetime import date
from datetime import datetime
import json
from kafka_helper import connect_kafka_producer, publish_message
from production_log_helper import production_log_record_dict, production_log_time_format
import sys
import numpy as np
import pandas as pd

# if __name__ == '__main__':
topic_name = 'production_log_test'
print('Publishing records..')
payload_records = []
# producer = connect_kafka_producer()
# date_seed = date.today().strftime(production_log_time_format)
date_seed = "1900-01-01" 

for i in range(1, 10000):
        # payload = production_log_record_dict(date_seed, i)
        payload = production_log_record_dict(date_seed)
        payload_records.append(payload)
        # date_seed = json.loads(payload)['end']
        date_seed = payload['end']
        # print(payload)
        # publish_message(producer, topic_name, '70:3', payload)

print(sys.getsizeof(payload_records.__sizeof__))

# df = pd.DataFrame(payload_records, columns=[
#                     'id', 'version', 'start', 'end'])
df = pd.DataFrame(payload_records)
print(df.info())
print(df.head())
