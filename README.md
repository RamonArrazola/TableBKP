# PROTOTIPO
Este script es un prototipo fase 0 y será modificado varias veces hasta definir una version final

# IMPORTANTE!
Antes de ejecutar el script, asegurate de poseer las ligas de acceso en el archivo .Env
Si vas a consultar hacia SQL **asegurate de estar conectado a la VPN correspondiente**

# Previo a la ejecución
El script usa varias librerias para ejecutar la conexion y procesamiento de resultados desde SQL y ICM, una de ellas es SQLAlchemy
que pide tener el driver *ODBC Driver 17 for SQL Server*, este driver se instala automaticamente al instalar MSSQL Server.
En caso de no contar con MSSQL Server se puede instalar el driver manualmente
<https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver17>

# Dependencias
Para ejecutar este proyecto, necesitas tener instaladas las siguientes bibliotecas:

* `pandas`
* `sqlalchemy`
* `requests`
* `python-dotenv`

Puedes instalarlas usando `pip`:

En la terminal ejecuta:

**pip install -r requirements.txt**

# Como funciona?
* Este Script se puede ejecutar desde CLI o haciendo doble click en el archivo *Ejecutar.bat*
* Hace una consulta con joins a ICM 10 y SQL (ICM 9) para obtener la estructura de tablas, este query obtiene
  nombres de campos, tipos de datos de los mismos, llaves primarias y llaves foraneas de la tabla e informacion general de la misma
* Debido a que ICM se sobreescribirá, almacenamos el resultado del query de tablas en un CSV en una carpeta fuera de Code, si esta
  carpeta no existe (O el archivo mismo) se hará la consulta a ICM, de lo contrario se usara el CSV (que funge como Backup)
* Con la información lista de ambas fuentes hace una comparativa y detecta que tablas no estan registradas en el modelo Origen.
* El script contempla la posibilidad que no exista el componente (o carptea) destino donde pertenece la tabla, si alguna tabla cae en
  este escenario el script se encargará de crear la carpeta
* Una vez obtenida una lista de tablas que no estan registradas en el modelo Origen genera un JSON anidado que contiene la estructura de las tablas listas a enviar al API de ICM
* Envia la peticion a ICM generando las tablas contenidas en la lista
* Si la tabla no se puede crear (Motivos diferente a que la tabla ya exista) se almacenara la estructura del JSON
  y se almacenara en la carpeta de modelo correspondiente (Misma donde se almacenan los backups)
* Si se repite una ejecucion previa a otra ejecucion con errores (Las tablas no se pudieron crear por algun motivo diferente a que ya existian) el script
  preguntara si se quiere reintentar la insercion de esas tablas, en caso de contestar S se ejecutara un subproceso que intentara volver a crear las tablas
  si en este subproceso se logra crear la tabla, el programa eliminara de la lista de rechazadas a todas las tablas creadas, asi como su estructura JSON
* Todas las tablas rechazadas por que ya existen simplemente se ignorarán

