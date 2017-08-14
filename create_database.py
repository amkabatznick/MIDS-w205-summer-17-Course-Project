import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# Connect to the database
conn = psycopg2.connect(database="postgres", user="postgres", password="pass", host="localhost", port="5432")

#Create the Database
try:
    # CREATE DATABASE can't run inside a transaction
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    #Drop the database if it already exists
    cur.execute("DROP DATABASE IF EXISTS NYT")
    cur.execute("CREATE DATABASE NYT")
    cur.close()
    conn.close()
except:
    print "Could not create NYT"

conn = psycopg2.connect(database="NYT", user="postgres", password="pass", host="localhost", port="5432")
cur = conn.cursor()
cur.execute('''CREATE TABLE sections
       (section_id int PRIMARY KEY NOT NULL,
       section_name text NOT NULL);''')

cur.execute('''CREATE TABLE subsection
       (sub_section_id int PRIMARY KEY NOT NULL,
       sub_section_name text NOT NULL,
       section_id int REFERENCES sections(section_id)
       );''')

cur.execute('''CREATE TABLE facet_types
       (facet_id int PRIMARY KEY NOT NULL,
       facet_name text NOT NULL
       );''')

cur.execute('''CREATE TABLE article_details
       (article_id int PRIMARY KEY  NOT NULL,
       title text,
       url text,
       update_date timestamp,
       sub_section_id int REFERENCES subsection(sub_section_id),
       section_id int REFERENCES sections(section_id)
       );''')

cur.execute('''CREATE TABLE facet_details
       (facet_detail_id int PRIMARY KEY NOT NULL,
       facet_details text NOT NULL,
       article_id REFERENCES  article_details(article_id)
       );''')

conn.commit()
conn.close()
