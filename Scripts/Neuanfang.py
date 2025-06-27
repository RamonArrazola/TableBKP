from main import *

#Neuanfang significa "nuevo comienzo", que puede implicar una segunda oportunidad.

for modelo in modelos:
    with open(os.path.join(backupDir, modelo, 'RejectedTables', 'Rejected.txt'), 'w', encoding='utf-8') as f:
        for item in f:
            rechazadas.append(item)
    
    for tabla in rechazadas:
        #Obtenemos la estrictura de la tabla desde la carpeta de rechazadas
        structure = pd.read_json(os.path.join(backupDir, modelo, 'RejectedTables', f"{tabla}.json"), orient='records', force_ascii=False)
        #Obtenemos el header para la peticion
        header = getHeader(modelo, bearerToken)
        #Mandamos la peticion para crear la tabla
        status = postTable(apiurl, header, structure)

        if status == 201:
            print(f"Tabla {tabla} creada exitosamente.")
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