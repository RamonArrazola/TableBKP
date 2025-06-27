from sqlalchemy import create_engine
from config.variables import *
from Libs.ICMFunc import *
from Libs.SQLFunc import *
from Libs.Func import *
import subprocess

#///////////////////Validamos si existen tablas rechazadas de un a ejecución anterior
for modelo in modelos:
    if os.path.isfile(os.path.join(backupDir, modelo, 'Rechazadas.csv')):
        print(f"Existen tablas rechazadas de {modelo} de ejecuciones anteriores.")
        if input("Deseas crear la tablas rechazadas? (S/N): ") == 'S':
            subprocess.run(["python", os.path.join("Script", "Neuanfang.py")])

#///////////////////PETICIONES de CONSULTA A ICM
#Validamos si existe el backup, de lo contrario lo creamos (UTIL?)
if not os.path.isdir(backupDir):
    creaSubcarpetas(backupDir, modelos)
    for modelo in modelos:
        #Consulta de tablas ICM (Peticion al QueryTool del API)
         #Generamos el header para la peticion
        header = getHeader(modelo, bearerToken)
         #Generamos el payload para la peticion del query
        data = getPayload(queryTablesDesc)
         #Hacemos la peticion al API
        response = postQuery(apiurl, header, data)
        #Validamos la respuesta del API
        if response.status_code == 200:
            print(f"Tablas de {modelo} obtenidas correctamente!")
            #Normalizamos la resupuesta y construimos el DataFrame
            globals()['listaTablas' + modelo + 'ICM'] = construyeDF(pd.json_normalize(response.json()))
            #Almacenamos la consulta en el backup
            AlmacenaConsulta(backupDir, globals()['listaTablas' + modelo + 'ICM'], modelo)
        else:
            print(f"Ocurrio un error con {modelo}: {response.status_code} - {response.text}")
else:   #Si el backup ya existe, lo leemos
    for modelo in modelos:
        globals()['listaTablas' + modelo + 'ICM'] = pd.read_csv(os.path.join(backupDir, modelo, 'BackUpTablesStructure.csv'), sep=';')

#///////////////////PETICIONES de CONSULTA A SQL
#Conexion a SQL
for db in dbs:
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
        globals()['listaTablas' + db + 'SQL'] = SQLQuery(cnxn, queryTablesDesc)
    finally: #Cerramos la conexion y limpieamos el engine
        if cnxn:
            cnxn.dispose()
            print(f"Conexion SQL a {db} Cerrada")
            cnxn = None

#///////////////////Procesamiento de Tablas por cada Modelo
#Iteramos por cada modelo 
for modelo in modelos:
    #obtenemos el nombre homonimo del DF correspondiente
    db = obtieneHomonimo(modelo)
    #Comparacion de tabla ICM vs SQL
    InexistentesSQL = compareDataFrames(globals()['listaTablas' + modelo + 'ICM'], globals()['listaTablas' + db + 'SQL'])
    #Obtenemos una lista de las tablas inexistentes en SQL
    listaTablas = ListarInexistentesSQL(InexistentesSQL)
    #Obtenemos la estrucutra de las tablas de ListaTablas
    for table in listaTablas: 
        #Generamos el DataFrame de la tabla
        tableDF = InexistentesSQL[InexistentesSQL['TableName'] == table].copy()
        #Construimos el json anidado con la estructura de la tabla
        tableStc = construyeTable(table, tableDF)
        #PRUEBAS Imprimimos la estructura de la tabla
        print(json.dumps(tableStc))  #Comentar si no quieres ver la estructura de las tablas
        
#/////////////////////Peticiones para Crear las Tablas de ICM
        #Generamos el header para la peticion
        header = getHeader(modelo, bearerToken)
        #Mandamos la peticion para crear la tabla
        status = postTable(apiurl, header, tableStc)
        #Validamos el estado de la respuesta
        if status == 201:
            print(f"Tabla {table} creada exitosamente.")
        elif status == 0:
            print(f"Tabla {table} ya existe")
            continue
        else:
            print(f"Error al crear la tabla {table}.")
            print(f"{status} - {status.text}")
            #Almacenamos la tabla rechazada en una lista
            rechazadas.append(table)
            #Almacenamos el JSON de la estructura de la tabla
            pd.to_json(tableStc, os.path.join(backupDir, modelo, 'RejectedTables', f"{table}.json"), orient='records', force_ascii=False)
            continue

    if rechazadas.len() > 0:
        print(f"Las siguientes tablas de {modelo} no pudieron ser creadas: {', '.join(rechazadas)}")
        almacenaRechazadas(rechazadas, backupDir, InexistentesSQL, modelo)
        rechazadas = []  # Reiniciamos la lista de rechazadas para el siguiente modelo