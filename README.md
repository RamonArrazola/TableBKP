# PROTOTIPO
Este script es un prototipo fase 0 y será modificado varias veces hasta definir una version final

# IMPORTANTE!
Antes de ejecutar el script, **asegurate de estar conectado a la VPN correspondiente** y poseer las ligas de acceso en el archivo .Env

# Previo a la ejecución
El script usa varias librerias para ejecutar la conexion y procesamiento de resultados desde SQL y ICM, una de ellas es SQLAlchemy 
que pide tener el driver *ODBC Driver 17 for SQL Server*, este driver se instala automaticamente al instalar MSSQL Server. 
En caso de no contar con MSSQL Server se puede instalar el driver manualmente 
https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver17

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
- Este Script se puede ejecutar desde CLI o haciendo doble click en el archivo *Ejecutar.bat*
- Hace una consulta con joins a ICM 10 y SQL (ICM 9) para obtener la estructura de tablas, este query obtiene 
  nombres de campos, tipos de datos de los mismos, llaves primarias y llaves foraneas de la tabla e informacion general de la misma
- Debido a que ICM se sobreescribirá, almacenamos el resultado del query de tablas en un CSV en una carpeta fuera de Code, si esta
  carpeta no existe (O el archivo mismo) se hará la consulta a ICM, de lo contrario se usara el CSV (que funge como Backup)
- Con la información lista de ambas fuentes hace una comparativa y detecta que tablas no estan registradas en SQL. 
- Una vez obtenida una lista de tablas que no estan registradas en SQL genera un JSON anidado que contiene la estructura de las tablas listas a enviar al API de ICM 
- Envia la peticion a ICM generando las tablas contenidas en la lista 