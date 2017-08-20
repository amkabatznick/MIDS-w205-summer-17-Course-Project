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
    input_text = print('Database Exists\n')
    input_text = raw_input('Would you like to drop the database and recreate it: y/n? ')
    input_text = input_text.lower()
    if input_text == 'y':
        print('Pretending to do something')
    else:
        print('Keeping Existing Database')
        exit()


print('We got all the way here')
