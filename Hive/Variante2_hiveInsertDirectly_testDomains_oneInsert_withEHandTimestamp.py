#!/usr/bin/env python3

import requests
import json
import numpy as np
from pyhive import hive
import datetime as dt
import sys

# database name - must exist!
dbname = "mesdatapython"
api_url = "http://10.50.12.131:8080"

print("Start: " + str(dt.datetime.now()))

# Hive base
try:
    cursor = hive.connect(host="ubuha01.wi.lehre.mosbach.dhbw.de"
                        , port=10000, username="hive", password="admin"
                        , database=dbname, auth='CUSTOM').cursor()
except:
    print("Keine Verbindung zu Hive möglich. Die Ausführung wird abgebrochen.") 
    sys.exit() 
                    
# API base
try:
    url_meta = api_url + "/meta/"
    headers = {
    'Authorization': 'Basic UmVzdHVzZXI6S2VubndvcnQwNA==',
    'Cookie': 'JSESSIONID=AAC2EFBE19BC028C7CE932443375F13B'
    }
    response_meta = requests.request("GET", url_meta, headers=headers)
except:
    print("Keine Verbindung zur MES Hydra API möglich. Die Ausführung wird abgebrochen.") 
    sys.exit()
response_json_meta = json.loads(response_meta.text)

# list of domains and services to be queried:
def allDomainsAndServices(): # warning: duration for load and save all data ist very high!
    domainsAndServicesWithList = []
    for entry in response_json_meta:
        split = entry.split('.')
        if(split[1][ 0 : 4 ] == 'list'):
            domainsAndServicesWithList.append(split)
    return domainsAndServicesWithList
            
def specifiedDomainsAndServices():
    # enter domains and Services by your own:
    domainsAndServicesWithList = [
        #["BOOrder", "list"],        
        ["MDWorkplanOrder", "list"],
        ["MDWorkplanOperation", "list"],
        ["BOResource", "list"]
    ]
    return domainsAndServicesWithList

# start querying  
counter = 0

for entry in allDomainsAndServices():
    # send request, get response
    try:
        url_data = api_url + "/data/" + str(entry[0]) + "/" + str(entry[1])
        response_data = requests.request("GET", url_data, headers=headers, timeout=60)
    except:
        print(str(entry[0]) + "_" + str(entry[1]) + " ist nicht erreichbar. Die Domain wird übersprungen.") 
        continue
    response_json_data = json.loads(response_data.text)
    entryTimestamp = str(dt.datetime.now())

    if(response_json_data):
        # get metainfos
        metainfos = response_json_data[0]
        datatype = metainfos['__type']
        
        tblname = str(entry[0]) + "_" + str(entry[1]) + "_" + str(datatype)

        if(datatype != 'ERROR'):
            metadata = metainfos['data']
            columns = "entrytimestamp TIMESTAMP"
            for col in metadata:
                coltype = col['type']
                if(coltype == "DATETIME"):
                    coltype = "TIMESTAMP"
                
                # attention: . in col-names are repaced by _    
                columns = columns + ", " + col['name'].replace(".", "_") + " " + coltype
            
            try:
                # create table
                #cursor.execute("DROP TABLE IF EXISTS " + dbname + "." + tblname)
                hql = "CREATE TABLE IF NOT EXISTS " + dbname + "." + tblname + " (" + columns + ")"
                cursor.execute(hql)

                allValues = ""
                
                for row in response_json_data:
                    if row['__rowType'] == 'DATA':
                        # insert data in table
                        vals = row['data']
                        vals.insert(0, entryTimestamp)
                        strVals = str(vals)
                        allValues = allValues + "(" + strVals[1:len(strVals)-1] + "), "

                allValues = allValues[:len(allValues)-2]
                hql = "INSERT INTO " + dbname + "." + tblname + " VALUES " + allValues
                cursor.execute(hql.replace('None', 'null'))
            except:
                print("Abspeicherung im Hive nicht möglich. Domain wird übersprungen.")
                print(str(entry[0]) + "_" + str(entry[1]) + ': FEHLER ' + str(dt.datetime.now()))
            else:
                counter+=1
                print(str(entry[0]) + "_" + str(entry[1]) + ": Domain gespeichert (" + str(counter) + ") " + str(dt.datetime.now()))
        else: print(str(entry[0]) + "_" + str(entry[1]) + ': FEHLER ' + str(dt.datetime.now()))
    else: print(str(entry[0]) + "_" + str(entry[1]) + ': leer ' + str(dt.datetime.now()))

cursor.close()
print("Fertig! " + str(dt.datetime.now()))

input("Zum Beenden eine Taste drücken...")