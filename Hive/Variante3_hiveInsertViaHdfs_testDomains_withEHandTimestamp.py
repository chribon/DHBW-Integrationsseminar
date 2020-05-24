#!/usr/bin/env python3

import requests
import json
import numpy as np
from pyhive import hive
import hdfs
import datetime as dt
import sys

dbname = "mesdatapythonviahdfs"
api_url = "http://10.50.12.131:8080"

print("Start: " + str(dt.datetime.now()))

# Hive base
try:
    cursor = hive.connect(host="ubuha01.wi.lehre.mosbach.dhbw.de"
                        , port=10000, username="hive", password="admin"
                        , database="mesdatapythonviahdfs", auth='CUSTOM').cursor()
except:
    print("Keine Verbindung zu Hive möglich. Die Ausführung wird abgebrochen.") 
    sys.exit() 

# HDFS base
try:
    client_hdfs = hdfs.InsecureClient('http://ubuhama.wi.lehre.mosbach.dhbw.de:50070', user="hive")
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
    response_meta = requests.request("GET", url_meta, headers=headers, timeout=60)
except:
    print("Keine Verbindung zur MES Hydra API möglich. Die Ausführung wird abgebrochen.") 
    sys.exit()
response_json_meta = json.loads(response_meta.text)

# list of domains and services to be queried:
def allDomainsAndServices():
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
        
        tblname = str(entry[0]) + "_" + str(entry[1])

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
                cursor.execute("DROP TABLE IF EXISTS " + dbname + ".data_tt")
                hql = "CREATE TABLE " + dbname + ".data_tt (" + columns + ") ROW FORMAT DELIMITED FIELDS TERMINATED BY ';' STORED AS TEXTFILE"
                cursor.execute(hql)
                
                # -----------DATA-------------
                # save data as csv in hdfs
                data_toSave = []
                for row in response_json_data:
                    if row['__rowType'] == 'DATA':
                        datarow = row['data']
                        datarow.insert(0, entryTimestamp)
                        data_toSave.append(datarow)
                hdfs_path = "tmp/data/mesdata/data.csv"
                with client_hdfs.write(hdfs_path, encoding = 'utf-8', overwrite=True) as writer:
                    np.savetxt(writer, data_toSave, delimiter=";", fmt='%s')
                
                # insert data in data_tt
                cursor.execute("LOAD DATA INPATH 'hdfs://ubuhama.wi.lehre.mosbach.dhbw.de:8020/user/hive/tmp/data/mesdata/' INTO TABLE " + dbname + ".data_tt")
                client_hdfs.delete('tmp/data/mesdata/', recursive=True)
                
                # insert data in final table
                #cursor.execute("DROP TABLE IF EXISTS " + dbname + "." + tblname)
                cursor.execute("CREATE TABLE IF NOT EXISTS " + dbname + "." + tblname + " (" + columns + ")" )
                cursor.execute("INSERT INTO TABLE " + dbname + "." + tblname + " SELECT * FROM " + dbname + ".data_tt")
                cursor.execute("DROP TABLE IF EXISTS " + dbname + ".data_tt")
            except:
                print(str(entry[0]) + "_" + str(entry[1]) + ': Abspeicherung im Hive nicht möglich. Domain wird übersprungen. ' + str(dt.datetime.now()))
            else:
                counter+=1
                print(str(entry[0]) + "_" + str(entry[1]) + ": Domain gespeichert. (Gesamt gespeichert: " + str(counter) + " Stück) " + str(dt.datetime.now()))
        else: print(str(entry[0]) + "_" + str(entry[1]) + ': API liefert Fehler, manuelle Parameterauswahl erforderlich. Domain wird übersprungen. ' + str(dt.datetime.now()))
    else: print(str(entry[0]) + "_" + str(entry[1]) + ': leer. Domain wird übersprungen. ' + str(dt.datetime.now()))

cursor.close()
print("Fertig! " + str(dt.datetime.now()))
#input("Zum Beenden eine Taste drücken...")