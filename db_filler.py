import psycopg2
import random


def row():
    return {
        "user_id": random.randint(1, 100),
        "event_id": random.randint(1, 100),
        "amount": random.randint(-100000, 100000)
    }

print(row())
print(row())
print(row())

# conn = psycopg2.connect("dbname=postgres user=postgres")
# cursor = conn.cursor()
#
#
# conn.commit()
#
# cursor.close()
# conn.close()
