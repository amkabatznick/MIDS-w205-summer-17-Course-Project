from NytCredentials import NYTimesApi
import requests
import psycopg2

def _return_field_details(conn, Value,Table):
    TableMapping = {
                    'subsections':{
                                    'id':'sub_section_id',
                                    'CheckColumn': 'sub_section_name'
                                },
                    'facet_types':{
                                    'id':'facet_id',
                                    'CheckColumn': 'facet_name'
                                },
                    'facet_details':{
                                    'id':'facet_detail_id',
                                    'CheckColumn': 'facet_details'
                                }
                    }

    cur = conn.cursor()
    cur.execute("SELECT id from %s WHERE %s=%s", (Table,TableMapping[Table]['CheckColumn'],Value))
    if not cur.rowcount:
        cur.execute("INSERT INTO %s Values (%s) RETURNING %s" (Table, Value,TableMapping[Table]['id']))
        conn.commit()


    return cur.fetchone()[0]



#Establishes Database Connection
conn = psycopg2.connect(database="nyt", user="postgres", password="pass", host="localhost", port="5432")

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
    section_id
    subsection = i['subsection']
    sub_section_id = _retn_field_details(conn,subsection,'facet_details')

    cur.execute("INSERT INTO article_details Values ('title','url','update_date','section_id','sub_section_id') RETURNING 'article_id'",
        (title,url, update_date,section_id, sub_section_id))
    article_id = cur.fetchone()[0]

        for j in i.keys():
          #Check if this is a facet
          if 'facet' in j:
              #Get this FacetId
              facet_type_id = _return_field_details(conn, j,'facet_types')

              #See if this facet has information
              if i[j]:
                #If its a person update this information
                if j == 'per_facet':
                    for per in i['per_facet']:
                        per = per.split(',')
                        name = per[1].split()[0]+' '+per[0]ur
                        facet_details_id = _return_field_details(conn,name,'facet_details'))
                        cur.execute("INSERT INTO article_facet_details Values
                            ('article_id','facet_id','facet_detail_id')",
                            (article_id,facet_type_id,facet_details_id))
                else:
                    for facet in i[j]:
                        facet_details_id = _return_field_details(conn,facet,'facet_details'))
                        cur.execute("INSERT INTO article_facet_details Values
                        ('article_id','facet_id','facet_detail_id')",
                        (article_id,facet_type_id,facet_details_id))

        conn.commit()
