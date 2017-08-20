import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# Connect to the database
conn = psycopg2.connect(database="postgres", user="postgres", password="pass", host="localhos
t", port="5432")

#Create the Database

try:
    # CREATE DATABASE can't run inside a transaction
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    cur.execute("CREATE DATABASE twitterdata")
    cur.close()
    conn.close()
except:
    print "Could not create tcount"

#Connecting to tcount

conn = psycopg2.connect(database="tcount", user="postgres", password="pass", host="localhost"
, port="5432")


cur = conn.cursor()
cur.execute('''CREATE TABLE tweets
       (id SERIAL PRIMARY KEY NOT NULL,
        tweet TEXT NOT NULL,
        date DATE NOT NULL);''')
conn.commit()
