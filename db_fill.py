import random
import logging

from psycopg2 import IntegrityError, InternalError, connect

conn = connect("dbname=postgres user=postgres")
cursor = conn.cursor()

increment = 0
DB_CHUNK_SIZE = 1000

cursor.execute(f"SELECT count(*) FROM raw_data;")
rows = cursor.fetchone()[0]
logging.warning(f"Current raw_data count: {rows}.")

if rows > 90000:
    logging.warning("Table is full.")
else:
    while increment <= DB_CHUNK_SIZE:

        user_id = random.randint(1, 1001)
        event_id = random.randint(1, 1001)

        try:
            cursor.execute(
                f"INSERT INTO raw_data (user_id, event_id, amount) "
                f"VALUES ({user_id}, {event_id}, {random.randint(-100000, 100000)});"
            )
            conn.commit()
        except (IntegrityError, InternalError):
            conn.rollback()

        increment += 1

        if increment == 1000:
            cursor.execute(f"SELECT count(*) FROM raw_data;")
            rows = cursor.fetchone()[0]
            logging.warning(f"Current raw_data count: {rows}.")

            if rows > 90000:
                increment = 10000
            else:
                increment = 0


conn.commit()

cursor.close()
conn.close()
