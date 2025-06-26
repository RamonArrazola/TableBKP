import pandas as pd

def getConnStr(uid:str, pwd:str, sqlServer:str, db: str):
    conn_str = (f"mssql+pyodbc://{uid}:{pwd}@{sqlServer}/{db}?driver=ODBC+Driver+17+for+SQL+Server")
    return conn_str

def SQLQuery(cnxn: any, query: str):
    df = pd.read_sql(query, cnxn)

    if df.empty:
        print("No data returned from SQL query.")
    else:
        print(f"Data retrieved successfully with {len(df)} rows.")

    return df