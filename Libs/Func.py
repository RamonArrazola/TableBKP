import xml.etree.ElementTree as ET
from config.variables import base
import pandas as pd
import json
import os

def compareDataFrames(origen: pd.DataFrame, destino: pd.DataFrame):
    #Tomando en cuenta que el origen siempre tendra mas tablas que destino 
    diferentes = origen[~origen['Order'].isin(destino['Order'])].copy()
    return diferentes

def listarInexistentesSQL(df: pd.DataFrame):
    uniqueTables = df['TableName'].unique()
    lista = uniqueTables.tolist()
    return lista

def construyeTable(name: str, table: pd.DataFrame):
    #Generamos el bloque Columns
    columns = []
    pickListFiltered = False
    #Si el DataFrame tiene en sus registros el campo EffStart_ o EffEnd_, los eliminamos
    if 'EffStart_' in table.columns:
        table = table.drop(columns=['EffStart_', 'EffEnd_'], errors='ignore')
    #Iteramos entre las columnas definidas en el DataFrame y generamos el bloque Column
    for _, row in table.iterrows(): 
        col = {
            "column": {
            "name": row['ColumnName'],
            "tpye": row['Type'],
            "isKey": bool(row['IsKey']),
            },
            "isKey": bool(row['IsKey'])
        }
        #Si la tabla tiene un campo PickList agregamos ReferencedTable
        if 'PickListTableName' in row and pd.notna(row['PickListTableName']) and str(row['PickListTableName']).strip() != '':
            col["column"]["referencedTable"] = row['PickListTableName']
        #Si la tabla tiene un campo PickListColumn agregamos ReferencedName
        if 'PickListColumnName' in row and pd.notna(row['PickListColumnName']) and str(row['PickListColumnName']).strip() != '':
            col["column"]["referencedName"] = row['PickListColumnName'] 
        #Si la tabla tiene un campo FilterID, lo agregamos como ReferencedName
        if 'Query' in row and pd.notna(row['Query']) and str(row['Query']).strip() != '':
            filtro = row['Query']
            query = xmlJson(filtro)
            col["column"]["source"] = query
            pickListFiltered = True
        #Aladimos la columna al bloque Columns
        columns.append(col)
    #Generamos el bloque Table
    table = {
        "table": {
            "name": name,
            "tableType": "Custom",
            "effectiveDated": False,
            "lastUpdatedTracking": True
        },
        "columns" : columns
    }
    #Generamos el bloque Bounds
    bounds = {
        "x": row['X'],
        "y": row['Y'],
        "width": row['Width'],
        "height": row['Height'],
    }
    #Constrimos el json Final
    jTable = {
        "name": name,
        "table": table,
        "bounds": bounds,
        "parentBlockId": row['ParentBlockID'],
    }
    #Si la tabla tiene un campo PickList, agregamos el bloque AdditionalData
    if pickListFiltered: 
        jTable["AdditionalData"] = {
            "href": f"/api/v1/tableadditionaldata/{name}"
        }
    return jTable

def creaSubcarpetas(dir: str, modelo: str):
    if not os.path.isdir(dir):
        os.makedirs(dir, exist_ok=True)
    os.makedirs(os.path.join(dir, modelo), exist_ok=True)

def almacenaConsulta(base: str, df: pd.DataFrame, modelo: str):
    df.to_csv(os.path.join(base, modelo, 'BackUpTablesStructure.csv'),sep=';', index=False)

def obtieneHomonimo(modelo: str):
    with open(os.path.join(base, '..', 'utils', 'EvilTwin.json'), 'r') as f:
        homonimos = json.load(f)
    return homonimos.get(modelo, modelo)

def obtienePRD(modelo: str):
    with open(os.path.join(base, '..', 'utils', 'EvilTwin.json'), 'r') as f:
        homonimos = json.load(f)
    prd = homonimos.get(modelo, modelo)
    return prd if prd != modelo else modelo

def delXmlNamespace(elem):
    # Elimina la línea de declaración XML
    elem = elem.lstrip().replace('<?xml version="1.0" encoding="utf-8"?>', '').lstrip()
    return elem

def parseSelect(selectElem):
    # Convierte un XML de definición de tabla a un diccionario Python con la estructura del bloque 'source'.
    items = []
    for item in selectElem.findall('.//{*}select-item'):
        items.append({
            "column": item.findtext('{*}column'),
            "table": item.findtext('{*}table'),
            "dataType": item.findtext('{*}datatype', default="None"),
            "type": "SelectItem"
        })
    return items

def parseWhere(whereElem):
    # Convierte la causa 'where' de la consulta, extrayendo restricciones y sus detalles.
    constraints = []
    clause = whereElem.find('{*}clause') if whereElem is not None else None
    if clause is not None:
        for constraint in clause.findall('{*}constraint'):
            constraints.append({
                "constraintType": "constraint",
                "op": constraint.findtext('{*}operator'),
                "dataFieldLeft": {
                    "expressionType": "dataField",
                    "table": constraint.findtext('{*}table'),
                    "column": constraint.findtext('{*}column'),
                    "isDate": False,
                    "literalType": "Integer"
                },
                "caseSensitive": True,
                "literalRight": {
                    "expressionType": "literal",
                    "isDate": False,
                    "literalType": constraint.find('{*}operand/{*}literal').attrib.get('type', 'String') if constraint.find('{*}operand/{*}literal') is not None else 'String',
                    "value": constraint.findtext('{*}operand/{*}literal')
                },
                "isPreciseDecimal": False,
                "escapeWildcards": False
            })
    return {
        "constraintType": "clause",
        "op": clause.attrib.get('type', 'and') if clause is not None else 'and',
        "caseSensitive": False,
        "constraints": constraints,
        "isPreciseDecimal": False,
        "escapeWildcards": False
    }

def parseQuery(queryElem):
    # Convierte el nodo <query> en un diccionario con la estructura esperada.
    selectElem = queryElem.find('{*}select')
    fromElem = queryElem.find('{*}from')
    whereElem = queryElem.find('{*}where')
    return {
        "selectItems": parseSelect(selectElem) if selectElem is not None else [],
        "source": parseFrom(fromElem) if fromElem is not None else {},
        "joins": [],
        "whereClause": parseWhere(whereElem) if whereElem is not None else {
            "constraintType": "clause",
            "op": "and",
            "caseSensitive": False,
            "constraints": [],
            "isPreciseDecimal": False,
            "escapeWildcards": False
        },
        "order": [],
        "group": [],
        "havingClause": {
            "constraintType": "clause",
            "op": "and",
            "caseSensitive": False,
            "constraints": [],
            "isPreciseDecimal": False,
            "escapeWildcards": False
        },
        "distinct": False
    }

def parseFrom(fromElem):
    # Convierte la sección 'from' de la consulta, identificando si es una subconsulta o una tabla.
    queryElem = fromElem.find('.//{*}query') if fromElem is not None else None
    if queryElem is not None:
        return {
            "query": parseQuery(queryElem),
            "alias": fromElem.findtext('.//{*}alias'),
            "sourceType": "Query"
        }
    tableElem = fromElem.find('.//{*}table') if fromElem is not None else None
    if tableElem is not None:
        return {
            "namespaceTable": {"name": tableElem.text},
            "alias": fromElem.findtext('.//{*}alias'),
            "sourceType": "Table"
        }
    return {}

def xmlJson(xml: str) -> dict:
    # Eliminar la línea de declaración XML si existe
    clean = delXmlNamespace(xml)
    #Formatear el XML para que sea compatible con ElementTree
    root = ET.fromstring(clean)
    # Buscar el primer query sin importar el namespace
    queryElem = root.find('.//{*}query')
    if queryElem is None:
        raise ValueError("No se encontró el nodo <query> en el XML.")
    sourceDict = {
        "source": {
            "query": parseQuery(queryElem),
            "sourceNames": {}
        }
    }
    return sourceDict

def almacenaRechazadas(rechazadas: list, dir: str, df: pd.DataFrame, modelo: str):
    #Si no existe. crea un archivo .txt llamado Rejected.txt, de lo contrario lo sobreescribe
    if not os.path.exists(os.path.join(dir, modelo, 'RejectedTables', 'Rejected.txt')):
        with open(os.path.join(dir, modelo, 'RejectedTables', 'Rejected.txt'), 'w', encoding='utf-8') as f:
            f.write("Tablas Rechazadas:\n")
            for item in rechazadas:
                f.write(f"{item}\n")
    #Filtramos el DataFrame para obtener las tablas rechazadas
    rechazadasDF = df[df['TableName'].isin(rechazadas)].copy()
    #Guardamos el DataFrame de tablas rechazadas en un CSV
    rechazadasDF.to_csv(os.path.join(dir, modelo, 'RejectedTables.csv'), sep=';', index=False, encoding='utf-8')

def listaComponentes(df: pd.DataFrame):
    #Obtenemos los campos unicos de la columna Name del DataFrame
    carpetas = df['Name'].unique().tolist()
    #Filtramos el DataFrame para obtener solo los registros que tienen un Name en la lista de carpetas, ademas de solo copiar los campos Name y ParentBlockID
    componentes = df[df['Name'].isin(carpetas)][['Name', 'ParentBlockID']].drop_duplicates(subset=['Name']).copy()
    #Generamos una columna nueva llamado AbueloBlockID
    componentes['AbueloBlockID'] = None
    #Nos quedamos solamente con las columnas Name, ParentBlockID y AbueloBlockID
    return componentes

def construyeComponente(name: str, parentBlockId: int): 
    #Construimos el bloque blockDefinition
    blockDefinition = {
        "name": name
    }
    #Definimos el color de la carpeta
    color =  {
        "a": 255,
        "b": 61,
        "g": 188,
        "r": 251
    }
    #Estructuramos el JSON basico para la carpeta
    data = {
        "blockDefinition": blockDefinition,
        "parentBlockId": parentBlockId,
        "color": color,
        "isVisible": True
    }

    return data

def actualizaParentBlock(df: pd.DataFrame, parche: pd.DataFrame):
    #Iteramos por cada fila del DataFrame de tablas ICM
    for row in parche.iterrows():
        #Actualizamos el parentBlockId en el DataFrame de tablas ICM
        df.loc[df['Name'] == row[1]['Name'], 'ParentBlockID'] = row[1]['ParentBlockID']
    # Retornamos el DataFrame actualizado
    return df
