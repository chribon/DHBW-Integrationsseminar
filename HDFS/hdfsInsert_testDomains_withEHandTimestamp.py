#!/usr/bin/env python3

import requests
import json
import numpy as np
import hdfs
import datetime as dt
import sys

api_url = "http://10.50.12.131:8080"
hdfs_folder = "/tmp/data/WI17A_MES/python/"

print("Start: " + str(dt.datetime.now()))

# HDFS base
try:
    client_hdfs = hdfs.InsecureClient('http://ubuhama.wi.lehre.mosbach.dhbw.de:50070', user="admin")
except:
    print("Keine Verbindung zum HDFS möglich. Die Ausführung wird abgebrochen.") 
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
        #["MDWorkplanOrder", "list"],
        #["MDWorkplanOperation", "list"],
        #["BOResource", "list"],
        #["SamplingPlan", "list"]
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
    entryTimestamp = str(dt.datetime.now().strftime("%Y-%m-%d_%H-%M-%S"))

    if(response_json_data):
        # get metainfos
        metainfos = response_json_data[0]
        datatype = metainfos['__type']

        if(datatype != 'ERROR'):
            metadata = metainfos['data']
            metadatalist = []
            for col in metadata:
                metadatalist.append(col['name'] + "|" + col['type'])

            data_toSave = []
            data_toSave.append(metadatalist)

            for row in response_json_data:
                if row['__rowType'] == 'DATA':
                    data_toSave.append(row['data'])
            try:
                hdfs_path = hdfs_folder + str(entry[0]) + "_" + str(entry[1]) + "_" + str(datatype) + "_" + entryTimestamp + ".csv"
                with client_hdfs.write(hdfs_path, encoding = 'utf-8', overwrite=True) as writer:
                    np.savetxt(writer, data_toSave, delimiter=";", fmt='%s')
            except:
                print("Abspeicherung im HDFS nicht möglich. Domain wird übersprungen.")
                print(str(entry[0]) + "_" + str(entry[1]) + ': FEHLER ' + str(dt.datetime.now()))
            else:
                counter+=1
                print(str(entry[0]) + "_" + str(entry[1]) + ": Domain gespeichert (" + str(counter) + ") " + str(dt.datetime.now()))
        else: print(str(entry[0]) + "_" + str(entry[1]) + ': FEHLER ' + str(dt.datetime.now()))
    else: print(str(entry[0]) + "_" + str(entry[1]) + ': leer ' + str(dt.datetime.now()))
    
print("Fertig! " + str(dt.datetime.now()))

input("Zum Beenden eine Taste drücken...")
