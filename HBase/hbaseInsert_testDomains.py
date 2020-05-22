import requests
import json
import paramiko
import happybase

# API info
url_meta = "http://10.50.12.131:8080/meta/"
headers = {
  'Authorization': 'Basic UmVzdHVzZXI6S2VubndvcnQwNA==',
  'Cookie': 'JSESSIONID=AAC2EFBE19BC028C7CE932443375F13B'
}
response_meta = requests.request("GET", url_meta, headers=headers)
response_json_meta = json.load(response_meta.text)

classes_and_methods = [
    ["BOOrder", "list"],
    ["BOResource", "list"],
    ["MDWorkplanOrder", "list"]
]

