import json
import random
from datetime import date
from datetime import timedelta
from datetime import datetime

#production_log_test:0:31002: key=b'70:3' value={'id': 7, 'version': 2, 'start': '2019-10-21', 'end': '2019-10-26'}

production_log_time_format =  "%Y-%m-%d"

def production_log_record_json(date_seed, id):
    return json.dumps(
        production_log_record_dict(date_seed)
    , default=str)

def production_log_record_dict(date_seed):
    date_one = datetime.strptime(date_seed, production_log_time_format )
    rand_days = random.randint(1, 10)
    date_two = date_one + timedelta(days=rand_days)
    return {
        'id': id,
        'version': 2,
        'start': date_one.strftime(production_log_time_format),
        'end': date_two.strftime(production_log_time_format)
    }


def add_duration(msg_dict):
    start_date = datetime.strptime(msg_dict["start"], production_log_time_format)
    end_date = datetime.strptime(msg_dict["end"], production_log_time_format)
    duration_days = (end_date - start_date).days
    msg_dict.update(duration_days = duration_days)
    return json.dumps(msg_dict)

