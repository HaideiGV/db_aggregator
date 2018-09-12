import psycopg2

conn = psycopg2.connect("dbname=postgres user=postgres")
cursor = conn.cursor()

cursor.execute(
    "CREATE TABLE raw_data ("
    "id serial primary key, "
    "user_id integer, "
    "event_id integer, "
    "amount integer, "
    "CONSTRAINT unique_user_id_event_id UNIQUE(user_id, event_id));"
)

cursor.execute(
    "CREATE TABLE agg_data ("
    "user_id integer primary key, "
    "balance integer, "
    "event_number integer, "
    "best_event_id integer, "
    "worst_event_id integer);"
)

cursor.execute(
    "CREATE TABLE last_processed_id (processed_id integer primary key);"
)

conn.commit()

cursor.close()
conn.close()
