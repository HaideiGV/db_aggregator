import psycopg2

conn = psycopg2.connect("dbname=postgres user=postgres")
cursor = conn.cursor()

cursor.execute("DROP TABLE raw_data")
cursor.execute("DROP TABLE agg_data;")
cursor.execute("DROP TABLE last_processed_id;")

conn.commit()

cursor.close()
conn.close()
