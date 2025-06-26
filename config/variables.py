from dotenv import load_dotenv
import os

#Variables de entorno
base = os.path.dirname(os.path.abspath(__file__))
dotenv_path = os.path.join(base, '..', 'Env', '.Env')
load_dotenv(dotenv_path)
backupDir = os.path.join(base, '../..', 'TablesBackup')

#Diccionario de tipado de datos
sqlTypeMap = os.path.join(base, '..', 'utils', 'sqlTypeMap.json')

#Variaables para las peticiones al API  
bearerToken = os.environ.get('bearerToken')
modelos = [m.strip() for m in os.environ.get('modelos', '').split(',') if m.strip()]
dbs = [d.strip() for d in os.environ.get('dbs', '').split(',') if d.strip()]
    
apiurl = os.environ.get('apiurl')

queryTablesDesc = r"""SELECT 
	CT."TableName",
	CT."TableType",
	CT."EffectiveDated",
	--CT."RowVersion",
	CC."ColumnName", 
	CC."Type",
	CC."Order",
	CC."IsKey",
	CPL."TableName" AS "PickListTableName",
	CPL."ColumnName" AS "PickListColumnName",
	CPL."Name" AS "PickListName",
	CPL."FilterID",
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
	--PB."_RowVersion" AS "BaseRowVersion",
	PB."Name", 
	PB."Type" AS "BaseType"
FROM "CustomTable" CT
INNER JOIN "CustomColumn" CC ON CC."TableName" = CT."TableName"
LEFT JOIN "CustomPickList" CPL ON CPL."TableName" = CT."TableName"
 AND CPL."ColumnName" = CC."ColumnName"
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
		--E."RowVersion",
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