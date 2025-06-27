import xml.etree.ElementTree as ET
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
    PickListFiltered= False
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
            query = xml_json(filtro)
            col["column"]["source"] = query
            PickListFiltered = True
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
    if PickListFiltered: 
        jTable["AdditionalData"] = {
            "href": f"/api/v1/tableadditionaldata/{name}"
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

def del_xml_namespace(elem):
    # Elimina la línea de declaración XML
    elem = elem.lstrip().replace('<?xml version="1.0" encoding="utf-8"?>', '').lstrip()
    return elem


def parse_select(select_elem):
    # Convierte un XML de definición de tabla a un diccionario Python con la estructura del bloque 'source'.
    items = []
    for item in select_elem.findall('.//{*}select-item'):
        items.append({
            "column": item.findtext('{*}column'),
            "table": item.findtext('{*}table'),
            "dataType": item.findtext('{*}datatype', default="None"),
            "type": "SelectItem"
        })
    return items

def parse_where(where_elem):
    # Convierte la causa 'where' de la consulta, extrayendo restricciones y sus detalles.
    constraints = []
    clause = where_elem.find('{*}clause') if where_elem is not None else None
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

def parse_query(query_elem):
    # Convierte el nodo <query> en un diccionario con la estructura esperada.
    select_elem = query_elem.find('{*}select')
    from_elem = query_elem.find('{*}from')
    where_elem = query_elem.find('{*}where')
    return {
        "selectItems": parse_select(select_elem) if select_elem is not None else [],
        "source": parse_from(from_elem) if from_elem is not None else {},
        "joins": [],
        "whereClause": parse_where(where_elem) if where_elem is not None else {
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

def parse_from(from_elem):
    # Convierte la sección 'from' de la consulta, identificando si es una subconsulta o una tabla.
    query_elem = from_elem.find('.//{*}query') if from_elem is not None else None
    if query_elem is not None:
        return {
            "query": parse_query(query_elem),
            "alias": from_elem.findtext('.//{*}alias'),
            "sourceType": "Query"
        }
    table_elem = from_elem.find('.//{*}table') if from_elem is not None else None
    if table_elem is not None:
        return {
            "namespaceTable": {"name": table_elem.text},
            "alias": from_elem.findtext('.//{*}alias'),
            "sourceType": "Table"
        }
    return {}

def xml_json(xml: str) -> dict:
    # Eliminar la línea de declaración XML si existe
    clean = del_xml_namespace(xml)
    #Formatear el XML para que sea compatible con ElementTree
    root = ET.fromstring(clean)
    # Buscar el primer query sin importar el namespace
    query_elem = root.find('.//{*}query')
    if query_elem is None:
        raise ValueError("No se encontró el nodo <query> en el XML.")
    source_dict = {
        "source": {
            "query": parse_query(query_elem),
            "sourceNames": {}
        }
    }
    return source_dict

def almacenaRechazadas(rechazadas: list, dir: str, df: pd.DataFrame, modelo: str):
    #Generamos la subcarpeta para almacenar las tablas rechazadas
    os.makedirs(os.path.join(dir, modelo, 'RejectedTables'), exist_ok=True)
    #Si no existe. crea un archivo .txt llamado Rejected.txt, de lo contrario lo sobreescribe
    if not os.path.exists(os.path.join(dir, modelo, 'RejectedTables', 'Rejected.txt')):
        with open(os.path.join(dir, modelo, 'RejectedTables', 'Rejected.txt'), 'w', encoding='utf-8') as f:
            f.write("Tablas Rechazadas:\n")
            for item in rechazadas:
                f.write(f"{item}\n")
    #Filtramos el DataFrame para obtener las tablas rechazadas
    rechazadasDF = df[df['TableName'].isin(rechazadas)].copy()
    #Guardamos el DataFrame de tablas rechazadas en un CSV
    rechazadasDF.to_csv(os.path.join(dir, modelo, 'RejectedTables', 'RejectedTables.csv'), sep=';', index=False, encoding='utf-8')