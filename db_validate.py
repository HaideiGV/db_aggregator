import logging
import random
from psycopg2 import InternalError, IntegrityError, connect

conn = connect("dbname=postgres user=postgres")
cursor = conn.cursor()

cursor.execute("select max(user_id), min(user_id) from raw_data;")
count = cursor.fetchone()

if count:
    rnd_ids = ','.join([str(random.randint(count[1], count[0])) for i in range(1, 11)])
    cursor.execute(
        f"SELECT count(user_id) FROM agg_data as AD "
        f"where user_id in ({rnd_ids}) group by user_id "
        f"having balance = (select sum(amount) from raw_data where user_id = AD.user_id group by user_id) and "
        f"event_number = (select count(event_id) from raw_data where user_id = AD.user_id group by user_id) and "
        f"best_event_id = ("
        f"select event_id from raw_data where user_id = AD.user_id group by event_id order by max(amount) desc limit 1"
        f") and "
        f"worst_event_id = ("
        f"select event_id from raw_data where user_id = AD.user_id group by event_id order by min(amount) desc limit 1"
        f")"
    )
    rows = cursor.fetchall()

    if len(rows) != 10:
        logging.warning("Data is not valid.")
    else:
        logging.warning("Data is valid.")

cursor.close()
conn.close()
