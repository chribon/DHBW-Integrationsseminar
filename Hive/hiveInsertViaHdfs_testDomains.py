import requests
import json
import numpy as np
from pyhive import hive
import hdfs
import datetime as dt

print("Start: " + str(dt.datetime.now()))

# Hive base
cursor = hive.connect(host="ubuha01.wi.lehre.mosbach.dhbw.de"
                      , port=10000, username="hive", password="admin"
                      , database="mesdatapythonviahdfs", auth='CUSTOM').cursor()
      

# HDFS base
client_hdfs = hdfs.InsecureClient('http://ubuhama.wi.lehre.mosbach.dhbw.de:50070', user="hive")
                     
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
        
        dbname = "mesdatapythonviahdfs"
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
            cursor.execute("DROP TABLE IF EXISTS " + dbname + ".data_tt")
            sql = "CREATE TABLE " + dbname + ".data_tt (" + columns[2:] + ") ROW FORMAT DELIMITED FIELDS TERMINATED BY ';' STORED AS TEXTFILE"
            cursor.execute(sql)
            
            # -----------DATA-------------
            # save data as csv in hdfs
            data_toSave = []
            for row in response_json_data:
                if row['__rowType'] == 'DATA':
                    data_toSave.append(row['data'])
            hdfs_path = "tmp/data/mesdata/data.csv"
            with client_hdfs.write(hdfs_path, encoding = 'utf-8', overwrite=True) as writer:
                np.savetxt(writer, data_toSave, delimiter=";", fmt='%s')
            
            # insert data in data_tt
            cursor.execute("LOAD DATA INPATH 'hdfs://ubuhama.wi.lehre.mosbach.dhbw.de:8020/user/hive/tmp/data/mesdata/' INTO TABLE " + dbname + ".data_tt")
            client_hdfs.delete('tmp/data/mesdata/', recursive=True)
            
            # insert data in final table
            cursor.execute("DROP TABLE IF EXISTS " + dbname + "." + tblname)
            cursor.execute("CREATE TABLE " + dbname + "." + tblname + " AS SELECT * FROM " + dbname + ".data_tt")
            cursor.execute("DROP TABLE IF EXISTS " + dbname + ".data_tt")
            
            counter+=1
            print(str(entry[0]) + "_" + str(entry[1]) + ": domain saved (" + str(counter) + ") " + str(dt.datetime.now()))
        else: print(str(entry[0]) + "_" + str(entry[1]) + ': ERROR ' + str(dt.datetime.now()))
    else: print(str(entry[0]) + "_" + str(entry[1]) + ': leer ' + str(dt.datetime.now()))

cursor.close()
print("Fertig! " + str(dt.datetime.now()))
