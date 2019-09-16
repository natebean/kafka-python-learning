from datetime import date
from datetime import datetime
import time
import json
from production_log_helper import production_log_record_dict, production_log_time_format
import sys
from sqlite_test import sql_connection
import random

# if __name__ == '__main__':
topic_name = 'production_log_test'
print('starting up')
payload_records = []
date_seed = "1900-01-01" 

con = sql_connection()
cur = con.cursor()
t0 = time.time()
print('connection open')
for i in range(1, 4000000):
        if i == 1:
                t1 = time.time()
                print("starting", flush=True)
        payload = production_log_record_dict(date_seed)
        start_int = int(payload["start"].replace("-",""))
        end_int = int(payload["end"].replace("-",""))
        cur.execute("INSERT INTO production_log VALUES (?, ?, ?, ?)",
        [ random.randint(1, 100), random.randint(1,350), start_int, end_int]
        )
        if i % 2000000 == 0:
                t2 = time.time()
                delta = (t2 - t1)
                t1 = t2
                print("count: %d duration:%d" % (i, delta), flush= True)
        date_seed = payload['end']

con.commit()
cur.execute('''CREATE INDEX "pl_sid_id_idx" ON "production_log" (
	"sid_id",
	"version"
)''')
con.close()
tfinal = time.time()
delta = (tfinal - t0)
print("final duration:%d" % (delta), flush= True)
