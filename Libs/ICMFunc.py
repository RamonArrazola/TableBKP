import requests as rq
import pandas as pd
import json
import os

sqlTypeMap = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'utils', 'sqlTypeMap.json')

def getHeader(model: str, bearer: str):
    header = {
        "Authorization": f"Bearer {bearer}",
        "Content-Type": "application/json",
        "Model": model
    }
    return header

def getPayload(query: str):
    data = {
        "queryString": query,
        "offset": 0,
        "limit": 0
    }
    return data

def postQuery(apiurl: str, header: dict, data: dict):
    url = apiurl + '/rpc/querytool'
    response = rq.post(url, headers = header, json = data)

    if response.status_code == 200:
        print(f"Success!")
    else:
        print(f"Error: {response.status_code} - {response.text}")

    return response

def construyeDF(jResponse: any):
    columns = [col['name'] for col in jResponse['columnDefinitions'][0]]
    types = [col['type'] for col in jResponse['columnDefinitions'][0]]

    with open(sqlTypeMap, 'r') as f:
        type_map =json.load(f)

    type_dict = {col: type_map.get(typ.lower(), 'object') for col, typ in zip(columns, types)}

    DataFrame = pd.DataFrame(jResponse['data'][0], columns=columns).astype(type_dict)
    
    return DataFrame

def getTable(apiurl: str, table: str, header: dict, data: dict):
    url = apiurl + '/customtables/' + table + '/inputforms/0/data?offset=0&limit=0'
    response = rq.get(url, headers = header, json = data)

    if response.status_code == 200:
        print(f"Success!")

    return response 

def postTable(apiurl: str, header: dict, data: dict):
    url = apiurl + '/customtables'
    response = rq.post(url, headers = header, json = data)

    if response.status_code == 201:
        print(f"Table created successfully!")
    else:
        print(f"Error creating table: {response.status_code} - {response.text}")
        
    return response.status_code