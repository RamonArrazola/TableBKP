from sqlalchemy import create_engine
import pandas as pd

def getConnStr(uid:str, pwd:str, sqlServer:str, db: str):
    conn_str = (f"mssql+pyodbc://{uid}:{pwd}@{sqlServer}/{db}?driver=ODBC+Driver+17+for+SQL+Server")
    return conn_str

def SQLQuery(cnxn: any, query: str):
    df = pd.read_sql(query, cnxn)

    if df.empty:
        print("No se retornaron datos de la consulta.")
    else:
        print(f"Datos recuperados exitosamente. {len(df)} rows.")

    return df

def consultaSQL(uid: str, pwd: str, sqlServer: str, db: str, query: str):
    #NOTA: 
    #La consulta a SQL solo es para obtener las tablas desde SQL Server, las tablas se seguiran creando en un modelo ICM destino
    try:
        #Obtenemos la cadena de conexion
        conn_str = getConnStr(uid, pwd, sqlServer, db)
        #Generamos el engine de conexion a SQL
        cnxn = create_engine(conn_str)
        if cnxn:
            print(f"Conexion SQL a {db} Establecida")
        else:
            print(f"Error al establecer la conexion SQL a {db}")
        #Obtenermos la informacion desde SQL
        df = SQLQuery(cnxn, query)
    finally: #Cerramos la conexion y limpieamos el engine
        if cnxn:
            cnxn.dispose()
            print(f"Conexion SQL a {db} Cerrada")
            cnxn = None
    return df