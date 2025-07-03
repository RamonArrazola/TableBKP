from Libs.Func import obtieneHomonimo, construyeComponente
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
        print(f"Query Ejecutado correctamente!")
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
        print(f"Exito al obtener la tabla {table}!")

    return response 

def postTable(apiurl: str, header: dict, data: dict):
    url = apiurl + '/customtables'
    response = rq.post(url, headers = header, json = data)

    if response.status_code == 201:
        print(f"Tabla creada correctamente!")
    else:
        print(f"Error al crear la tabla: {response.status_code} - {response.text}")
        
    return response

def obtieneComponentes(apiurl: str, bearerToken: str, modelo: str):
    header = getHeader(modelo, bearerToken)
    url = apiurl + '/components'
    response = rq.get(url, headers=header)

    if response.status_code == 200:
        print(f"Carpetas de {modelo} obtenidas correctamente!")
        return response.json()
    else:
        print(f"Error al obtener las carpetas de {modelo}: {response.status_code} - {response.text}")
        return None
    
def validaComponentes(apiurl: str, bearerToken: str, modelo: str, componentesNecesarios: pd.DataFrame):
    #Generamos una lista de carpetas a validar
    listaCarpetas = componentesNecesarios['Name'].tolist()
    #Recuperamos los componentes a buscar y los formateamos para la consulta SQL
    buscados = ',\n'.join([f"('{c}')" for c in listaCarpetas])
    #Construimos la consulta SQL
    query= rf"""SELECT "NOT"."Carpeta"
                FROM (
                        VALUES
                        {buscados}
                    ) AS "NOT"("Carpeta")
                LEFT JOIN "BaseBlock" AS p ON "NOT"."Carpeta" = p."Name"
                WHERE p."BlockID" IS NULL"""
    #Obtenemos el destino 
    prd = obtieneHomonimo(modelo)
    #Generamos el header
    header = getHeader(prd, bearerToken)
    #Generamos el payload
    data = getPayload(query)
    #Hacemos la peticion al API
    response = postQuery(apiurl, header, data)
    #Validamos la respuesta del API
    if response.status_code == 200:
        print(f"Carpetas de {modelo} Obtenidas correctamente!")
        dataJson = response.json()
        listaInexistentes = [item[0] for item in dataJson.get("data", [])]
        componentesInexistentes = componentesNecesarios[componentesNecesarios['Name'].isin(listaInexistentes)]

    return componentesInexistentes

def obtieneAbueloID(apiurl: str, bearerToken: str, modelo: str, blockID: str):
    #Obtenemos el ParentBlockID del Abuelo
    url = apiurl + f'/components/{blockID}'
    header = getHeader(modelo, bearerToken)
    response = rq.get(url, headers=header)

    if response.status_code == 200:
        abueloData = response.json()
        return abueloData['parentBlockId']
    else:
        print(f"Error al obtener parentBlockId para {blockID}: {response.status_code} - {response.text}")
        return None

def postComponente(apiurl: str, bearerToken: str, modelo: str, row: pd.Series):
    #Creamos la URL para la peticion
    url = apiurl + '/components'
    #Creamos el payload para la peticion
    data = construyeComponente(row[1]['Name'], row[1]['AbueloBlockID'])
    #Generamos el header para la peticion
    header = getHeader(modelo, bearerToken)
    #Hacemos la peticion al API
    response = rq.post(url, headers=header, json=data)
    if response.status_code == 200:
        print(f"Carpeta {row[1]['Name']} creada correctamente!")
        #Formateamos la respuesta a JSON
        responseData = response.json()
        #Retornamos el BlockID del componente creado
        return responseData['blockDefinition']['blockId']
    else:
        print(f"Error al crear la carpeta {row['Name']}: {response.status_code} - {response.text}")

def creaComponentes(apiurl: str, bearerToken: str, modelo: str, component: pd.DataFrame):
    #Obtenemos una lista de ID's de los componentes a crear
    blocks = component['ParentBlockID'].tolist()
    #Recorremos la lista de carpetas
    for block in blocks:
        #Obtenemos el AbueloBlockID de cada carpeta
        AbueloBlockID = obtieneAbueloID(apiurl, bearerToken, modelo, block)
        #Asignamos el AbueloBlockID a la columna AbueloBlockID del DataFrame
        component.loc[component['ParentBlockID'] == block, 'AbueloBlockID'] = AbueloBlockID
    #Ordenamos el DataFrame por AbueloBlockId de menor a mayor
    component = component.sort_values(by='AbueloBlockID', ascending=True).reset_index(drop=True)
    #Validamos si alguna carpeta depende de otra en este mismo DataFrame
    if component['AbueloBlockID'].isin(component['ParentBlockID']).any():
        #Creamos un DF con las carpetas que dependen de otras carpetas que aun no se han creado
        dependientes = component[component['AbueloBlockID'].isin(component['ParentBlockID'])]
        #Separamos las carpetas que no dependen de otras
        component = component[~component.index.isin(dependientes.index)].reset_index(drop=True)
    #Obtenemos el modelo origen 
    prd = obtieneHomonimo(modelo)
    #Si existen carpetas dependientes, las procesamos por separado
    if 'dependientes' in locals() and not dependientes.empty:
        #Primero creamos las carpetas que no dependen de otras carpetas por crear
        for row in component.iterrows():
            #Creamos el componente y obtenemos su nuevo BlockID
            newBlockId = postComponente(apiurl, bearerToken, prd, row)
            #Actualizamos el AbueloBlockID de las carpetas con dependencias
            dependientes.loc[dependientes['AbueloBlockID'] == row['AbueloBlockID'], 'AbueloBlockID'] = newBlockId
            #Actualizamos el blockID de las carpetas credas
            component.loc[component['Name'] == row['Name'], 'ParentBlockID'] = newBlockId
        #Luego creamos las carpetas que dependen de otras
        for row in dependientes.iterrows():
            #Creamos el componente y obtenemos su nuevo BlockID
            newBlockId = postComponente(apiurl, bearerToken, prd, row)
            #Actualizamos el blockID de las carpetas credas
            component.loc[component['Name'] == row['Name'], 'ParentBlockID'] = newBlockId
        #Por ultimo, regresamos las carpetas con dependencias al DataFrame original
        component = pd.concat([component, dependientes], ignore_index=True)
        #Reordenamos el DataFrame por AbueloBlockId de menor a mayor
        component = component.sort_values(by='AbueloBlockID', ascending=True).reset_index(drop=True)
    #Si no hay dependencias, creamos las carpetas directamente
    else: 
        #Creamos las carpetas directamente si no hay dependencias
        for row in component.iterrows():
            #Creamos el componente y obtenemos su nuevo BlockID
            newBlockId = postComponente(apiurl, bearerToken, prd, row)
            #Actualizamos el blockID de las carpetas credas
            component.loc[component['Name'] == row[1]['Name'], 'ParentBlockID'] = newBlockId
            #Reordenamos el DataFrame por AbueloBlockId de menor a mayor
            component = component.sort_values(by='AbueloBlockID', ascending=True).reset_index(drop=True)
    #Eliminamos el AbueloBlockID del DataFrame, ya que no es necesario para las tablas
    component = component.drop(columns=['AbueloBlockID'])
    #Retornamos el DataFrame con los BlockID's actualizados
    return component