import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from sys import exit

#Private Function for Use Later On
def _cleanSections(SectionString):
    SectionString = SectionString.replace(" " ,"")
    SectionString = SectionString.split(',')
    #SectionString = ["'"+i+"'" for i in SectionString]
    #return ",".join(SectionString)
    return SectionString

# Connect to the database
conn = psycopg2.connect(database="postgres", user="postgres", password="pass", host="localhost", port="5432")

#Create the Database
try:
    # CREATE DATABASE can't run inside a transaction
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) = 0 FROM pg_catalog.pg_database WHERE datname = 'nyt'")
    not_exists_row = cur.fetchone()
    not_exists = not_exists_row[0]
    if not_exists:
        cur.execute("CREATE DATABASE nyt")
        cur.close()
        conn.close()
    else:
        print('Database Exists\n')
        input_text = raw_input('Would you like to drop the database and recreate it: y/n? ')
        input_text = input_text.lower()
        if input_text == 'y':
            #Drop the database if it already exists
            cur.execute("DROP DATABASE IF EXISTS nyt")
            cur.execute("CREATE DATABASE nyt")
            cur.close()
            conn.close()
        else:
            print('Keeping Existing Database')
            cur.close()
            conn.close()
            exit()
except:
    print "Could not create nyt"

conn = psycopg2.connect(database="nyt", user="postgres", password="pass", host="localhost", port="5432")
cur = conn.cursor()
cur.execute('''CREATE TABLE sections
       (section_id SERIAL PRIMARY KEY NOT NULL,
       section_name text NOT NULL);''')

cur.execute('''CREATE TABLE subsections
       (sub_section_id SERIAL PRIMARY KEY NOT NULL,
       sub_section_name text NOT NULL
       );''')

cur.execute('''CREATE TABLE facet_types
       (facet_id SERIAL PRIMARY KEY NOT NULL,
       facet_name text NOT NULL
       );''')

cur.execute('''CREATE TABLE article_details
       (article_id SERIAL PRIMARY KEY  NOT NULL,
       title text NOT NULL,
       url text not null,
       update_date timestamp,
       section_id int REFERENCES sections(section_id),
       sub_section_id int REFERENCES subsections(sub_section_id),
       UNIQUE(title)
       );''')

cur.execute('''CREATE TABLE facet_details
       (facet_detail_id SERIAL PRIMARY KEY NOT NULL,
       facet_details text NOT NULL
       );''')

cur.execute('''CREATE TABLE article_facet_details
       (
            id Serial Not NUll,
            article_id int REFERENCES article_details(article_id),
            facet_id int REFERENCES facet_types (facet_id),
            facet_detail_id int REFERENCES facet_details(facet_detail_id),
            primary key (article_id,facet_id,facet_detail_id)
       );'''
)
cur.execute('''CREATE TABLE tweets
       (id SERIAL PRIMARY KEY NOT NULL,
        tweet TEXT NOT NULL,
        date TIMESTAMP NOT NULL,
        facet_detail_id INT NOT NULL,
        user_id BIGINT NOT NULL);''')



conn.commit()

#This Data can be found here https://developer.nytimes.com/top_stories_v2.json#/README.
#This Data does not currently update dynamically, so for now the list will need to be copied over
#Consider making more dynamic via Beautiful Soup
Sections = 'home, arts, automobiles, books, business, fashion, food, health, insider, magazine, movies, national, nyregion, obituaries, opinion, politics, realestate, science, sports, sundayreview, technology, theater, tmagazine, travel, upshot, world'
SectionString = _cleanSections(Sections)

for d in SectionString:
    cur.execute("Insert into sections (section_name) Values (%s);", (d,) )

conn.commit()

conn.close()
