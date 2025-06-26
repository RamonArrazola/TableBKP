from config.variables import base
import pandas as pd
import json
import os

def compareDataFrames(ICM: pd.DataFrame, SQL: pd.DataFrame):
    #Tomando en cuenta que ICM siempre tendra mas registros que SQL 
    diferentes = ICM[~ICM['Order'].isin(SQL['Order'])].copy()
    return diferentes

def ListarInexistentesSQL(df: pd.DataFrame):
    df = df['TableName'].unique()
    lista = df.tolist()
    return lista

def construyeTable(name: str, table: pd.DataFrame):
    #Generamos el bloque Columns
    columns = []
    for _, row in table.iterrows(): 
        col = {
            "column": {
            "name": row['ColumnName'],
            "tpye": row['Type'],
            "isKey": bool(row['IsKey']),
            },
            "isKey": bool(row['IsKey'])
        }
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
    
    return jTable

def creaSubcarpetas(dir: str, modelos: list):
    os.makedirs(dir, exist_ok=True)
    for modelo in modelos:
        modelo_dir = os.path.join(dir, modelo)
        os.makedirs(modelo_dir, exist_ok=True)

def AlmacenaConsulta(base: str, df: pd.DataFrame, modelo: str):
    df.to_csv(os.path.join(base, modelo, 'BackUpTablesStructure.csv'),sep=';', index=False)

def obtieneHomonimo(modelo: str):
    with open(os.path.join(base, '..', 'utils', 'EvilTwin.json'), 'r') as f:
        homonimos = json.load(f)
    return homonimos.get(modelo, modelo)