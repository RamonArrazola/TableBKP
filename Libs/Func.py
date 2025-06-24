import pandas as pd

def compareDataFrames(ICM: pd.DataFrame, SQL: pd.DataFrame):
    #Tomando en cuenta que ICM siempre tendra mas registros que SQL 
    diferentes = ICM[~ICM['Order'].isin(SQL['Order'])].copy()
    return diferentes

def ListarInexistentesSQL(df: pd.DataFrame):
    df = df['TableName'].unique()
    lista = df.tolist()
    return lista

def construyeTable(parentBlockId: int, bounds: dict, name: str, table: pd.DataFrame):
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

    #Constrimos el json Final
    jTable = {
        "name": name,
        "table": table,
        "bounds": bounds,
        "parentBlockId": parentBlockId,
    }
    
    return jTable