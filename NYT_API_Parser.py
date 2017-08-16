from NytCredentials import NYTimesApi
import requests
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

def _return_field_details(conn, Value,Table):
    TableMapping = {
                    'subsections':{
                                    'id':'sub_section_id',
                                    'CheckColumn': 'sub_section_name',
                                    'GetSql':"SELECT %s from subsections WHERE %s=%s",
                                    'InsertSql':"INSERT INTO subsections (sub_section_name) Values (%s) RETURNING sub_section_id"
                                },
                    'facet_types':{
                                    'id':'facet_id',
                                    'CheckColumn': 'facet_name',
                                    'GetSql':"SELECT %s from facet_types WHERE %s=%s",
                                    'InsertSql':"INSERT INTO facet_types (facet_name) Values (%s) RETURNING facet_id"
                                },
                    'facet_details':{
                                    'id':'facet_detail_id',
                                    'CheckColumn': 'facet_details',
                                    'GetSql':"SELECT %s from facet_details WHERE %s=%s",
                                    'InsertSql':"INSERT INTO facet_details (facet_details) Values (%s) RETURNING facet_detail_id"
                                }
                    }

    cur = conn.cursor()
    cur.execute(TableMapping[Table]['GetSql'], (TableMapping[Table]['id'],TableMapping[Table]['CheckColumn'],Value))
    if Table == 'facet_details':
        print(cur.query)
        print(cur.rowcount)
    if not cur.rowcount:
        cur.execute(TableMapping[Table]['InsertSql'], (Value,))



    return cur.fetchone()[0]



#Establishes Database Connection
conn = psycopg2.connect(database="nyt", user="postgres", password="pass", host="localhost", port="5432")
conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
#conn.autocommit = True

#Gets News Paper Data for All Sections
cur = conn.cursor()
cur.execute("SELECT section_id, section_name from sections order by section_id")
sections = cur.fetchall()

for section in sections:
    section_id = section[0]
    section_name = section[1]
    r= requests.get('http://api.nytimes.com/svc/topstories/v2/%s.json?api-key=%s' %(section_name,NYTimesApi)).json()

    for i in r['results']:
        url = i['url']
        title = i['title']
        update_date = i['updated_date'][0:10]+' '+ i['updated_date'][11:19]
        if i['subsection']:
            subsection = i['subsection']
            sub_section_id = _return_field_details(conn,subsection,'subsections')

            cur.execute("INSERT INTO article_details (title,url,update_date,section_id,sub_section_id) Values(%s,%s,%s,%s,%s)  RETURNING article_id",
                (title,url, update_date,section_id, sub_section_id))
        else:
            cur.execute("INSERT INTO article_details (title,url,update_date,section_id) Values (%s,%s,%s,%s) RETURNING article_id",
                (title,url, update_date,section_id))

        article_id = cur.fetchone()[0]

        for j in i.keys():
          #Check if this is a facet
          if 'facet' in j:
              #Get this FacetId
              facet_name = j
              facet_type_id = _return_field_details(conn, facet_name,'facet_types')

              #See if this facet has information
              if i[j]:
                #If its a person update this information
                if j == 'per_facet':
                    for per in i['per_facet']:
                        per = per.split(',')
                        if len(per) > 1:
                            name = per[1].split()[0]+' '+per[0]
                        else:
                            name = per[0]
                        facet_details_id = _return_field_details(conn,name,'facet_details')
                        cur.execute("INSERT INTO article_facet_details (article_id,facet_id,facet_detail_id) Values(%s,%s,%s)",(article_id,facet_type_id,facet_details_id))
                else:
                    for facet in i[j]:
                        facet_details_id = _return_field_details(conn,facet,'facet_details')
                        cur.execute("INSERT INTO article_facet_details (article_id,facet_id, facet_detail_id) Values(%s,%s,%s)",(article_id,facet_type_id,facet_details_id))
        #conn.commit()
