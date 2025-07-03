from config.variables import modelosDev, backupDir, bearerToken, apiurl
from Libs.ICMFunc import *
import os
import re

# Neuanfang significa "nuevo comienzo",que implica una segunda oportunidad.
def Neuafang():
    rechazadas = []
    #Recorremos los modelos origen
    for modelo in modelosDev:
        #Abrimos el txt en modo lectura
        with open(os.path.join(backupDir, modelo, 'RejectedTables', 'Rejected.txt'), 'r', encoding='utf-8') as f:
            #Saltamos la primera linea
            next(f)
            for item in f:
                #Agregamos el contenido del txt a la lista de rechazadas
                rechazadas.append(item.strip())

        for tabla in rechazadas:
            #Obtenemos la estrictura de la tabla desde la carpeta de rechazadas
            with open(os.path.join(backupDir, modelo, 'RejectedTables', f"{tabla}.json"), 'r', encoding='utf-8') as f:
                structure = json.load(f)
            #Obtenemos el modelo PRD homonimo al modelo DEV
            prd = obtieneHomonimo(modelo) 
            #Obtenemos el header para la peticion
            header = getHeader(prd, bearerToken)
            #Mandamos la peticion para crear la tabla
            status = postTable(apiurl, header, structure)

            if status.status_code == 201:
                print(f"Tabla {tabla} creada exitosamente.")
                #Eliminamos el archivo de la tabla rechazada
                os.remove(os.path.join(backupDir, modelo, 'RejectedTables', f"{tabla}.json"))
                rechazadas.remove(tabla)
            elif status.status_code == 200 or (hasattr(status, "status_text") and re.search(r"already exists\.?$", status.status_text.get("Message", ""))):
                print(f"Tabla {tabla} ya existe")
                #Eliminamos el archivo de la tabla rechazada
                os.remove(os.path.join(backupDir, modelo, 'RejectedTables', f"{tabla}.json"))
                rechazadas.remove(tabla)
            else:
                print(f"Error al crear la tabla {tabla}.")
                print(f"{status} - {status.text}")
    #Si quedaron tablas rechazadas, las almacenamos en el archivo Rejected.txt
    if len(rechazadas) > 0:
        with open(os.path.join(backupDir, modelo, 'RejectedTables', 'Rejected.txt'), 'a', encoding='utf-8') as f:
            f.write("Tablas Rechazadas:\n")
            for item in rechazadas:
                f.write(f"{item}\n")
    #Limpiamos la lista de tablas rechazadas
    rechazadas = []