import psycopg2

# Connect to the database
conn = psycopg2.connect(database="postgres", user="postgres", password="pass", host="localhost", port="5432")

cur = conn.cursor()
cur.execute("SELECT COUNT(*) = 0 FROM pg_catalog.pg_database WHERE datname = 'nyt'")
not_exists_row = cur.fetchone()
not_exists = not_exists_row[0]
if not_exists:
    print('Database does not exits')
else:
    print('Database exists')
