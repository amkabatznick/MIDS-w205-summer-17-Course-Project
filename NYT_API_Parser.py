from NytCredentials import NYTimesApi
import requests

#Get a List of Facets from database

#Get Sections from the database
#sections = ['sports']

#Get SubSections from the Database

#Get List of People in the Database

for i in sections:
    r= requests.get('http://api.nytimes.com/svc/topstories/v2/%s.json?api-key=%s' %(section,NYTimesApi)).json()
    for i in r['results']:
        for j in i.keys():
            if 'facet' in j:
                #Check if that facet is in the exiting list if not upload it


                #Once uploaded add the facet_details
                #Check if the facet detail is null
                if i[j]:
                    facet_details = i[j]

                    """
                        If its the person facet remove birthdays
                        and correct the order of the name to First Last
                        Rather than Last, First. Also removes middle names
                    """
                    if j == 'per_facet':
                        People = per.split(',')
                        for per in People:
                            name = per[1].split()[0]+' '+per[0]
                            
      url = i['url']
      title = i['title']

      #Subsection Might Be Null
      subsection = i['subsection']
      #If its not Null Check if the subsection is in the database

      #Date needs to be formatted
      update_date = i['updated_date'][0:10]+' '+ i['updated_date'][11:19]
