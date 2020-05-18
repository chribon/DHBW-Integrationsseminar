import requests
import json
import numpy as np
from pyhive import hive
import datetime as dt

print("Start: " + str(dt.datetime.now()))

# Hive base
cursor = hive.connect(host="ubuha01.wi.lehre.mosbach.dhbw.de"
                      , port=10000, username="hive", password="admin"
                      , database="mesdatapython", auth='CUSTOM').cursor()
                     
# API base
url_meta = "http://10.50.12.131:8080/meta/"
headers = {
  'Authorization': 'Basic UmVzdHVzZXI6S2VubndvcnQwNA==',
  'Cookie': 'JSESSIONID=AAC2EFBE19BC028C7CE932443375F13B'
}
response_meta = requests.request("GET", url_meta, headers=headers)
response_json_meta = json.loads(response_meta.text)

# list of domains and services to be queried:
def allObjectsAndMethods(): # warning: duration for load and save all data ist very high!
    classesAndMethodsWithList = []
    for entry in response_json_meta:
        split = entry.split('.')
        if(split[1][ 0 : 4 ] == 'list'):
            classesAndMethodsWithList.append(split)
    return classesAndMethodsWithList
            
def specifiedObjectsAndMethods():
    # enter classes and methods by your own:
    classesAndMethodsWithList = [
        ["BOOrder", "list"],        
        ["MDWorkplanOrder", "list"],
        ["MDWorkplanOperation", "list"],
        ["BOResource", "list"]
    ]
    return classesAndMethodsWithList

# start querying  
counter = 0

for entry in specifiedObjectsAndMethods():
    # send request, get response
    url_data = "http://10.50.12.131:8080/data/" + str(entry[0]) + "/" + str(entry[1])
    response_data = requests.request("GET", url_data, headers=headers)
    response_json_data = json.loads(response_data.text)

    if(response_json_data):
        # get metainfos
        metainfos = response_json_data[0]
        datatype = metainfos['__type']
        
        dbname = "mesdatapython"
        tblname = str(entry[0]) + "_" + str(entry[1]) + "_" + str(datatype)

        if(datatype != 'ERROR'):
            metadata = metainfos['data']
            columns = ""
            for col in metadata:
                coltype = col['type']
                if(coltype == "DATETIME"):
                    coltype = "TIMESTAMP"
                
                # attention: . in col-names are repaced by _    
                columns = columns + ", " + col['name'].replace(".", "_") + " " + coltype
            
            # create table
            cursor.execute("DROP TABLE IF EXISTS " + dbname + "." + tblname)
            sql = "CREATE TABLE " + dbname + "." + tblname + " (" + columns[2:] + ")"
            cursor.execute(sql)
               
            for row in response_json_data:
                if row['__rowType'] == 'DATA':
                    # insert data in table
                    vals = row['data']
                    strVals = str(vals)
                    strVals = strVals[1:len(strVals)-1]

                    sql = "INSERT INTO " + dbname + "." + tblname + " VALUES (" + strVals + ")"
                    cursor.execute(sql.replace('None', 'null'))
                        
            counter+=1
            print(str(entry[0]) + "_" + str(entry[1]) + ": domain saved (" + str(counter) + ") " + str(dt.datetime.now()))
        else: print(str(entry[0]) + "_" + str(entry[1]) + ': ERROR ' + str(dt.datetime.now()))
    else: print(str(entry[0]) + "_" + str(entry[1]) + ': leer ' + str(dt.datetime.now()))
    
print("Fertig! " + str(dt.datetime.now()))
