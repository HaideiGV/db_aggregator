import logging
from psycopg2 import InternalError, IntegrityError, connect

conn = connect("dbname=postgres user=postgres")
cursor = conn.cursor()

cursor.execute("SELECT processed_id FROM last_processed_id limit 1;")
proc_id = cursor.fetchone()

last_processed_id = proc_id[0] if proc_id else 1
logging.warning(f"Fetched last_processed_id: {last_processed_id}.")

start = last_processed_id
finish = last_processed_id + 999

cursor.execute(
    f"SELECT distinct user_id FROM raw_data WHERE user_id BETWEEN {start} AND {finish};"
)
user_ids = [row[0] for row in cursor.fetchall()]
logging.warning(f"Collected users: {user_ids}.\n")

if user_ids:
    cursor.execute(f"SELECT user_id FROM agg_data;")
    existing_agg_user_ids = cursor.fetchall()
    agg_user_ids = [row[0] for row in existing_agg_user_ids]

    for user_id in user_ids:
        cursor.execute(
            f"SELECT sum(amount), count(event_id) FROM raw_data WHERE user_id = {user_id};"
        )
        balance, count = cursor.fetchone()
        logging.warning(f"Aggregated balance {balance}, events_count {count} for user({user_id}).")

        cursor.execute(
            f"SELECT event_id, max(amount) as best "
            f"FROM raw_data where user_id = {user_id} "
            f"GROUP BY event_id "
            f"ORDER by best desc "
            f"LIMIT 1;"
        )
        best_event_id, best_event_value = cursor.fetchone()
        logging.warning(f"Best event id({best_event_id}) with value: {best_event_value}.")

        cursor.execute(
            f"SELECT event_id, min(amount) as worst "
            f"FROM raw_data where user_id = {user_id} "
            f"GROUP BY event_id "
            f"ORDER by worst desc "
            f"LIMIT 1;"
        )
        worst_event_id, worst_event_value = cursor.fetchone()
        logging.warning(f"Worst event id({worst_event_id}) with value: {worst_event_value}.")

        try:
            if user_id not in agg_user_ids:
                cursor.execute(
                    f"INSERT INTO agg_data (user_id, balance, event_number, best_event_id, worst_event_id) "
                    f"VALUES ({user_id}, {balance}, {count}, {best_event_id}, {worst_event_id});"
                )
                conn.commit()
                logging.warning(f"Data for user({user_id}) inserted.")
            else:
                cursor.execute(
                    f"UPDATE agg_data SET "
                    f"balance = {balance}, "
                    f"event_number = {count}, "
                    f"best_event_id = {best_event_id}, "
                    f"worst_event_id = {worst_event_id} "
                    f"WHERE user_id = {user_id};"
                )
                conn.commit()
                logging.warning(f"Data for user({user_id}) updated.")
        except (IntegrityError, InternalError) as e:
            conn.rollback()
            logging.warning(f"Data wasn't wrote due to {str(e)}.")

    try:
        if proc_id:
            cursor.execute(f"update last_processed_id set processed_id = {finish};")
            conn.commit()
            logging.warning(f"Updated {finish} as last processes id.")
        else:
            cursor.execute(f"insert into last_processed_id (processed_id) values ({finish});")
            conn.commit()
            logging.warning(f"Inserted {finish} as last processes id.")
    except (IntegrityError, InternalError):
        conn.rollback()
        logging.warning(f"Last processed id wasn't updated due to {str(e)}.")

cursor.close()
conn.close()
