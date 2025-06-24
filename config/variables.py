from dotenv import load_dotenv
import os

base = os.path.dirname(os.path.abspath(__file__))
dotenv_path = os.path.join(base, '..' , 'Env', '.Env')
load_dotenv(dotenv_path)

sqlTypeMap = os.path.join(base, '..' ,'utils', 'sqlTypeMap.json')

apiurl = os.environ.get('apiurl')
bearerToken = os.environ.get('bearerToken')
model = os.environ.get('model')

sqlServer = os.environ.get('sqlServer')
database = os.environ.get('database')
uid = os.environ.get('uid')
pwd = os.environ.get('pwd')

conn_str = (f"mssql+pyodbc://{uid}:{pwd}@{sqlServer}/{database}?driver=ODBC+Driver+17+for+SQL+Server")
cnxn = None

header = {
    "Authorization": f"Bearer {bearerToken}",
    "Content-Type": "application/json",
    "Model": model
}

data = {
    'offset': 0,
    'limit': 0,
}

columns = ['TableName', 'TableType', 'ColumnName', 'Type', 'IsKey', 'TableName2', 'ColumnName1', 'Name', 'FilterID']

parentBlockId = 1134

bounds = {
    "bounds": {
        "x": -481.45000000000005,
        "y": -7.449999999999989,
        "width": 224,
        "height": 180
    }
}

queryTablesDesc = r"""SELECT * FROM "CustomTable" CT
INNER JOIN "CustomColumn" CC
ON CC."TableName" = CT."TableName"
LEFT JOIN "CustomPickList" CPL
ON CPL."TableName" = CT."TableName"
AND CPL."ColumnName" = CC."ColumnName"
ORDER BY CC."Order" ASC"""