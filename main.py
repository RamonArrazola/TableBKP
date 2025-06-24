from sqlalchemy import create_engine
from config.variables import *
from Libs.ICMFunc import *
from Libs.SQLFunc import *
from Libs.Func import *

#Consulta de tablas ICM (Peticion al QueryTool del API)
 ##Obtenemos el Payload para la peticion del query
data = getPayload(queryTablesDesc)
 ## Hacemos la peticion al API
response = postQuery(apiurl, header, data)
 ## Listamos las tablas Obtenidas del API de ICM
listaTablasICM = construyeDF(pd.json_normalize(response.json()))

#Consulta de tablas SQL
try: 
    cnxn = create_engine(conn_str)
    if cnxn:
        print("Conexion SQL Establecida")
    else: 
        print("Error al establecer la conexion SQL")
    
    #Obtenermos la informacion desde SQL
    listaTablasSQL = SQLQuery(cnxn, queryTablesDesc)
finally:
    if cnxn:
        cnxn.dispose()
        print("Conexion SQL Cerrada")

#Comparacion de tabla ICM vs SQL 
InexistentesSQL = compareDataFrames(listaTablasICM, listaTablasSQL)

#Obtenemos una lista de las tablas inexistentes en SQL
listaTablas = ListarInexistentesSQL(InexistentesSQL)

#Recorremos la lista de tablas inexistentes en SQL (Son las que necesitamos respaldar)
for table in listaTablas: 
    #Generamos un DataFrame para cada tabla contenida en la lista de Tablas con la estructura de tabla recuperada previamente de ICM
    globals()[table] = InexistentesSQL[InexistentesSQL['TableName'] == table].copy()
    globals()[table] = globals()[table][columns]

    #Construimos el json anidado con la estructura de la tabla
    tableStc = construyeTable(parentBlockId, bounds, table, globals()[table])
    
    #Mandamos la peticion para crear la tabla
    status = postTable(apiurl, header, tableStc)
    if status == 201:
        print(f"Tabla {table} creada exitosamente.")
    else:
        print(f"Error al crear la tabla {table}.")