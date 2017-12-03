import sys
import os
import json
import requests
import time
import re
from neo4j.v1 import GraphDatabase, basic_auth
from py2neo import Graph, Node, Relationship, authenticate
from urllib.parse import urlparse, urlunparse

def connect():
    
    url ="bolt://hobby-gmhcabifaghcgbkeaeeijial.dbs.graphenedb.com:24786"
    user = "salim"
    password = "b.lkkuPzLUVr2a.xQUlltaH016XjZeg"
    authenticate("hobby-jdigfinckhclgbkemfgjjial.dbs.graphenedb.com:24780", "salim", "b.lkkuPzLUVr2a.xQUlltaH016XjZeg")
    graph = Graph("bolt://hobby-jdigfinckhclgbkemfgjjial.dbs.graphenedb.com:24786", user="salim", password="b.lkkuPzLUVr2a.xQUlltaH016XjZeg", bolt=True, secure = True, http_port = 24789, https_port = 24780)
    print(graph.data("MATCH (a:Planets) RETURN a.name"))
    print(graph.data("MATCH (a:Species) RETURN a.name"))
    print(graph.data("MATCH (a:People) RETURN a.name"))
    return graph
    
def populateDataBase(): 
     
    graph = connect()
    urls = {'people': 'https://swapi.co/api/people/',
            'planets': 'https://swapi.co/api/planets/',
            'species': 'https://swapi.co/api/species/'}
    for who in urls.keys():
        index = 1
        data = requests.get(urls[who]).json() # result is now a dict
        print ("_________________________________________________")
        for row in data["results"]:
            if(who == 'people'):
                if row["species"][0] is not None:
                    specie = re.findall('\d+', row["species"][0])
                else:
                    specie = ['null']

                if row["homeworld"] is not None:
                    homeworld = re.findall('\d+', row["homeworld"][0])
                else:
                    homeworld = ['null'] 

                print(row["name"], index)
                a = Node("People", name=row["name"], specie=specie[0], homeworld = homeworld, index = index)
                graph.create(a)
                index+=1

            elif (who == 'species'):
                if row["homeworld"] is not None:
                    homeworld = re.findall('\d+', row["homeworld"])
                else:
                    homeworld = ['null']   

                print(row["name"], index)             
                a = Node("Species", name=row["name"], classification=row["classification"], homeworld = homeworld, index = index) 
                graph.create(a)  
                index+=1  

            else :               
                print(row["name"], index)
                a = Node("Planets", name=row["name"], climate=row["climate"], index = index) 
                graph.create(a)
                index+=1           
            
            
    




if __name__ == '__main__':

    populateDataBase()