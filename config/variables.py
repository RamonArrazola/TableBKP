from dotenv import load_dotenv
import os

#Variables de entorno
base = os.path.dirname(os.path.abspath(__file__))
dotenv_path = os.path.join(base, '..', 'Env', '.Env')
load_dotenv(dotenv_path)
backupDir = os.path.join(base, '../..', 'TablesBackup')

#Diccionario de tipado de datos
sqlTypeMap = os.path.join(base, '..', 'utils', 'sqlTypeMap.json')

#ICM VS SQL? Variable para pruebas ICM vs SQL
icmVsSql = False # Si es True, se comparan tablas de ICM Dev y Prod con SQL, si es False, solo se comparan tablas de ICM Dev y Prod entre si.

#Variaables para las peticiones al API  
bearerToken = os.environ.get('bearerToken')
modelosDev = [m.strip() for m in os.environ.get('modelosDev', '').split(',') if m.strip()]
modelosPrd = [m.strip() for m in os.environ.get('modelosPrd', '').split(',') if m.strip()]
dbs = [d.strip() for d in os.environ.get('dbs', '').split(',') if d.strip()]
    
apiurl = os.environ.get('apiurl')

queryTablesDesc = r"""SELECT 
	CT."TableName",
	CT."TableType",
	CT."EffectiveDated",
	CC."ColumnName", 
	CC."Type",
	CC."Order",
	CC."IsKey",
	CPD."Table" AS "PickListTableName",
	CPD."Column" AS "PickListColumnName",
	CPL."FilterID",
	CFL."Query", 
	PB."ElementID",
	PB."Comment", 
	PB."IsGlobal",
	PB."ParentBlockID",
	PB."X",
	PB."Y",
	PB."Width",
	PB."Height",
	PB."R",
	PB."G",
	PB."B",
	PB."Name", 
	PB."Type" AS "BaseType"
---Informacion de la Tabla
FROM "CustomTable" CT
--Informacion de Columnas de la Tabla
INNER JOIN "CustomColumn" CC ON CC."TableName" = CT."TableName"
--Informacion General de Picklist en la Tabla
LEFT JOIN "CustomPickList" CPL ON CPL."TableName" = CT."TableName"
 AND CPL."ColumnName" = CC."ColumnName"
--Informacion General de las Dependencias de PickList
LEFT JOIN "CustomPickListDependency" CPD ON CPD."PickListTable" = CPL."TableName"
 AND CPD."PickListColumn" = CPL."ColumnName"
LEFT JOIN "CustomPickListFilter" CFL ON CFL."ID" = CPL."FilterID"
--Informacion General del Componente donde existe la tabla
LEFT JOIN(
	SELECT 
		T."ElementID",
		T."TableName",
		T."Comment", 
		T."IsGlobal",
		E."ParentBlockID",
		E."X",
		E."Y",
		E."Width",
		E."Height",
		E."R",
		E."G",
		E."B",
		B."Name", 
		B."Type"
	FROM "ComposerTable"  T
	INNER JOIN "ComposerElement" E ON T."ElementID"  = E."ElementID"
	INNER JOIN "BaseBlock" B ON B."BlockID" = E."ParentBlockID"
	) PB ON PB."TableName" = CT."TableName" 
ORDER BY CC."Order" ASC""" 

#Variables para la conexion SQL
sqlServer = os.environ.get('sqlServer')
uid = os.environ.get('uid')
pwd = os.environ.get('pwd')

#Variables para la generacion del engine de conexion SQL
cnxn = None

#Lista para almacenar las talablas rechazadas
rechazadas = []