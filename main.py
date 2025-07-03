from Libs.Neuanfang import Neuafang
from config.variables import *
from Libs.ICMFunc import *
from Libs.SQLFunc import *
from Libs.Func import *
import re

#///////////////////Validamos si existen tablas rechazadas de un a ejecución anterior
for modelo in modelosDev:
    if os.path.isfile(os.path.join(backupDir, modelo, 'RejectedTables.csv')):
        print(f"Existen tablas rechazadas de {modelo} de ejecuciones anteriores.")
        if input("Deseas crear la tablas rechazadas? (S/N): ") == 'S'.lower():
            Neuafang()

#///////////////////PETICIONES de CONSULTA A ICM
for modelo in modelosDev:
    if not os.path.isdir(os.path.join(backupDir, modelo)):
        #Creamos la subcarpeta del modelo
        creaSubcarpetas(backupDir, modelo)
        #Generamos el header para la peticion de consulta
        header = getHeader(modelo, bearerToken)
        #Generamos el payload para la peticion del query
        data = getPayload(queryTablesDesc)
        #Hacemos la peticion al API
        response = postQuery(apiurl, header, data)
        #Validamos la respuesta del API
        if response.status_code == 200:
            print(f"Tablas de {modelo} obtenidas correctamente!")
            #Normalizamos la resupuesta y construimos el DataFrame
            globals()['listaTablas' + re.sub(r'(dev|prd)', '', modelo, flags=re.IGNORECASE) + 'Origen'] = construyeDF(pd.json_normalize(response.json()))
            #Almacenamos la consulta en el backup
            almacenaConsulta(backupDir, globals()['listaTablas' + re.sub(r'(dev|prd)', '', modelo, flags=re.IGNORECASE) + 'Origen'], modelo)
    #Si el backup del modelo ya existe, lo leemos
    else:
        globals()['listaTablas' + re.sub(r'(dev|prd)', '', modelo, flags=re.IGNORECASE) + 'Origen'] = pd.read_csv(os.path.join(backupDir, modelo, 'BackUpTablesStructure.csv'), sep=';')

#/////////////////////Peticiones de consulta a Modelo Origen
if icmVsSQL:
    for db in dbs:
        #Obtiene homonimo del modelo 
        modelo = obtieneHomonimo(db)
        #Genera el Dataframe desde SQL pero con el nombre del modelo ICM
        globals()['listaTablas' + re.sub(r'(dev|prd)', '', modelo, flags=re.IGNORECASE) + 'Destino'] = consultaSQL(uid, pwd, sqlServer, db, queryTablesDesc)
else:  #El Backup de tablas final será ICM dev VS ICM Prod, por ende las peticiones y comparaciones a SQL son para entornos de pruebas a menos que asi se solicite
    for modelo in modelosPrd:
        #Generamos el header para la peticion de consulta
        header = getHeader(modelo, bearerToken)
        #Generamos el payload para la peticion del query
        data = getPayload(queryTablesDesc)
        #Hacemos la peticion al API
        response = postQuery(apiurl, header, data)
        #Validamos la respuesta del API
        if response.status_code == 200:
            print(f"Tablas de {modelo} obtenidas correctamente!")
            #Normalizamos la resupuesta y construimos el DataFrame
            globals()['listaTablas' + re.sub(r'(dev|prd)', '', modelo, flags=re.IGNORECASE) + 'Destino'] = construyeDF(pd.json_normalize(response.json()))

#////////////////////Validación y Creación de Carpetas en el Modelo ICM (Componentes)
for modelo in modelosDev:
    #Listamos las carpetas (Componentes) existentes en csv con tablas de Origen
    carpetasNecesarias = listaComponentes(globals()['listaTablas' + re.sub(r'(dev|prd)', '', modelo, flags=re.IGNORECASE) + 'Origen'])
    #Validamos si existen las carpetas (Componentes) en el modelo destino y recibimos una lista de carpetas que no existen
    components = validaComponentes(apiurl, bearerToken, modelo, carpetasNecesarias)
    #Veficiamos si faltan carpetas en el modelo destino
    if len(components) > 0: 
        #Creamos las carpetas en el modelo destino
        parche = creaComponentes(apiurl, bearerToken, modelo, components)
        #Actualizamos el ParentBlockID en el DataFrame de tablas ICM Origen
        actualizaParentBlock(globals()['listaTablas' + re.sub(r'(dev|prd)', '', modelo, flags=re.IGNORECASE) + 'Origen'], parche)

#///////////////////Procesamiento de Tablas por cada Modelo
#Iteramos por cada modelo
for modelo in modelosDev:
    #Comparacion de tabla ICM vs SQL
    inexistentesSQL = compareDataFrames(globals()['listaTablas' + re.sub(r'(dev|prd)', '', modelo, flags=re.IGNORECASE) + 'Origen'], globals()['listaTablas' + re.sub(r'(dev|prd)', '', modelo, flags=re.IGNORECASE) + 'Destino'])
    #Obtenemos una lista de las tablas inexistentes en SQL
    listaTablas = listarInexistentesSQL(inexistentesSQL)
    #Obtenemos la estrucutra de las tablas de ListaTablas
    for table in listaTablas:
        #Generamos el DataFrame de la tabla
        tableDF = inexistentesSQL[inexistentesSQL['TableName'] == table].copy()
        #Construimos el json anidado con la estructura de la tabla
        tableStc = construyeTable(table, tableDF)

#/////////////////////Peticiones para Crear las Tablas de ICM
        #Generamos el header para la peticion
        prd = obtieneHomonimo(modelo)
        header = getHeader(prd, bearerToken)
        #Mandamos la peticion para crear la tabla
        status = postTable(apiurl, header, tableStc)
        #Validamos el estado de la respuesta
        if status.status_code == 201:
            print(f"Tabla {table} creada exitosamente.")
        elif status.status_code == 200 or (hasattr(status, "status_text") and re.search(r"already exists\.?$")):
            print(f"Tabla {table} ya existe")
            continue
        else:
            print(f"Error al crear la tabla {table}.")
            print(f"{status}")
            #Almacenamos la tabla rechazada en una lista
            rechazadas.append(table)
            #Generamos la subcarpeta para almacenar las tablas rechazadas
            os.makedirs(os.path.join(backupDir, modelo, 'RejectedTables'), exist_ok=True)
            #Almacenamos el JSON de la estructura de la tabla
            with open(os.path.join(backupDir, modelo, 'RejectedTables', table +'.json'), 'w', encoding='utf-8') as f:
                json.dump(tableStc, f, ensure_ascii=False, indent=2)
            continue
    #Validamos si hay tablas rechazadas para el modelo actual (Solo aplica para ICM vs ICM, de lo contrario ya se habran almacenado las tablas )
    if len(rechazadas) > 0 & icmVsSQL == False:
        print(f"Las siguientes tablas de {modelo} no pudieron ser creadas: {', '.join(rechazadas)}")
        almacenaRechazadas(rechazadas, backupDir, inexistentesSQL, modelo)
        rechazadas = []  # Reiniciamos la lista de rechazadas para el siguiente modelo