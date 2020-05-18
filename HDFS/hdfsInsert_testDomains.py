import requests
import json
import numpy as np
import hdfs
import datetime as dt

print("Start: " + str(dt.datetime.now()))

# HDFS base
client_hdfs = hdfs.InsecureClient('http://ubuhama.wi.lehre.mosbach.dhbw.de:50070', user="admin")

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
            
            hdfs_path = "/tmp/data/WI17A_MES/python/" + str(entry[0]) + "_" + str(entry[1]) + "_" + str(datatype) + ".csv"
            with client_hdfs.write(hdfs_path, encoding = 'utf-8', overwrite=True) as writer:
                np.savetxt(writer, data_toSave, delimiter=";", fmt='%s')
            
            counter+=1
            print(str(entry[0]) + "_" + str(entry[1]) + ": domain saved (" + str(counter) + ") " + str(dt.datetime.now()))
        else: print(str(entry[0]) + "_" + str(entry[1]) + ': ERROR ' + str(dt.datetime.now()))
    else: print(str(entry[0]) + "_" + str(entry[1]) + ': leer ' + str(dt.datetime.now()))
    
print("Fertig! " + str(dt.datetime.now()))
