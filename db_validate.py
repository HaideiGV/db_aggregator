import logging
import random
from psycopg2 import InternalError, IntegrityError, connect

conn = connect("dbname=postgres user=postgres")
cursor = conn.cursor()

cursor.execute("select max(user_id), min(user_id) from raw_data;")
count = cursor.fetchone()

if count:
    rnd_ids = [random.randint(count[1], count[0]) for i in range(1, 11)]
    cursor.execute("select max(id), min(id) from raw_data;")
    count = cursor.fetchone()
    print(rnd_ids)

cursor.close()
conn.close()
