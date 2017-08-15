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
       title text PRIMARY KEY NOT NULL,
       url text,
       update_date timestamp,
       section_id int REFERENCES sections(section_id)
       sub_section_id int REFERENCES subsections(sub_section_id)
       );''')

cur.execute('''CREATE TABLE facet_details
       (facet_detail_id SERIAL PRIMARY KEY NOT NULL,
       facet_details text NOT NULL
       );''')

cur.execute('''CREATE TABLE article
       (
            id Serial PRIMARY KEY Not NUll,
            article_id int REFERENCES article_details(article_id),
            facet_id int REFERENCES facet_types (facet_id),
            facet_detail_id int REFERENCES facet_details(facet_detail_id)
       );''')



conn.commit()

#This Data can be found here https://developer.nytimes.com/top_stories_v2.json#/README.
#This Data does not currently update dynamically, so for now the list will need to be copied over
Sections = 'home, arts, automobiles, books, business, fashion, food, health, insider, magazine, movies, national, nyregion, obituaries, opinion, politics, realestate, science, sports, sundayreview, technology, theater, tmagazine, travel, upshot, world'
SectionString = _cleanSections(Sections)

cur.execute("Insert into sections (section_name) Values (%s)", (SectionString,) )
conn.commit()

conn.close()


def _cleanSections(SectionString):
    SectionString = Sections.replace(' ','')
    SectionString = SectionString.split(',')
    SectionString = SectionString.replace(" " ,"")
    SectionString = SectionString.split(',')
    SectionString = ["'"+i+"'" for i in Sections]
    return ",".join(SectionString)
