import sys
import os
import json
import requests
import re
from py2neo import Graph, Node, Relationship, authenticate


def connect():
    
    url ="bolt://hobby-gmhcabifaghcgbkeaeeijial.dbs.graphenedb.com:24786"
    user = "salim"
    password = "b.lkkuPzLUVr2a.xQUlltaH016XjZeg"
    authenticate("hobby-jdigfinckhclgbkemfgjjial.dbs.graphenedb.com:24780", "salim", "b.lkkuPzLUVr2a.xQUlltaH016XjZeg")
    graph = Graph("bolt://hobby-jdigfinckhclgbkemfgjjial.dbs.graphenedb.com:24786", user="salim", password="b.lkkuPzLUVr2a.xQUlltaH016XjZeg", bolt=True, secure = True, http_port = 24789, https_port = 24780)
    print(graph.data("MATCH (a:Planets) RETURN a.index"))
    print(graph.data("MATCH (a:Species) RETURN a.homeworld"))
    print(graph.data("MATCH (a:People) RETURN a.homeworld"))
    return graph
    
def populateDataBase(): 
     
    graph = connect()
    urls = {'people': 'https://swapi.co/api/people/?page=',
            'species': 'https://swapi.co/api/species/?page=',
            'planets': 'https://swapi.co/api/planets/?page='}

    for i in range(9):
        for who in urls.keys():
            data = requests.get(urls[who]+ str(i)).json() # result is now a dict
            print (urls[who] + str(i))
            print ("_________________________________________________")
            try :
                for row in data["results"]:
                    if(who == 'people'):
                        try:
                            specie = re.findall('\d+', row["species"][0])
                        except IndexError:
                            specie = ['0']

                        if row["homeworld"] is not None:
                            homeworld = re.findall('\d+', row["homeworld"])
                        else:
                            homeworld = ['0'] 

                        if row["url"] is not None:
                            index = re.findall('\d+', row["url"])
                        else:
                            index = ['0'] 

                        a = Node("People", name=row["name"], specie=specie[0], homeworld = homeworld[0], index = index[0])
                        print(a)
                        graph.create(a)

                    elif (who == 'species'):
                        if row["homeworld"] is not None:
                            homeworld = re.findall('\d+', row["homeworld"])
                        else:
                            homeworld = ['0'] 

                        if row["url"] is not None:
                            index = re.findall('\d+', row["url"])
                        else:
                            index = ['0']  

                                 
                        a = Node("Species", name=row["name"], classification=row["classification"], homeworld = homeworld[0], index = index[0]) 
                        print(a)
                        graph.create(a)   

                    else :               
                        print(row["name"], index)

                        if row["url"] is not None:
                            index = re.findall('\d+', row["url"])
                        else:
                            index = ['0'] 

                        a = Node("Planets", name=row["name"], climate=row["climate"], index = index[0]) 
                        print(a)
                        graph.create(a)
                
            except KeyError: 
                continue  

def getOneFrom() :
    graph = connect() 
    graph.data("MATCH p=()-[r:BlongsTo]->(s:Species)WHERE s.name ='Human' RETURN p")

              


if __name__ == '__main__':

    connect()