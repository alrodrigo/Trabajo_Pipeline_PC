"""ajustes globales y constantes del sismtea de  extraccion P&C.
Este archivo centraliza las rutas y nombres de columnas para facilitar el mantenimiento"""
import sys
from pathlib import Path #importa una librería para manejar rutas de archivos
#detectamos si el programa corre como codigo.py o como ejecutable .exe
if getattr(sys, 'frozen', False):
    # Si es un ejecutable, obtenemos la ruta del directorio del ejecutable
    PROYECTO_RAIZ = Path(sys.executable).resolve().parent
else:
    # Si es un script, obtenemos la ruta del directorio del script
    PROYECTO_RAIZ = Path(__file__).resolve().parent.parent
#definimos rutas de archivos y carpetas
#rutas de prueba sin necesidad de estar en la red
FILESERVER_LOCAL =  (
    PROYECTO_RAIZ
    / "fileserver"
    / "01 INVERSIONES"
    / "02 POWER BI"
    / "02 Información Real Proyectado"
    / "02.- Tablas de Hechos Estimados"
)
#ruta red de Codelco (produccion)
FILESERVER_PRODUCCION = Path(
    r"\\TEVMRAFS04.tte.codelco.cl\Exploraciones"
    r"\03_GERENCIA_OP_FI\01 INVERSIONES\02 POWER BI"
    r"\02 Información Real Proyectado\02.- Tablas de Hechos Estimados"
)

#--rutas de salida (output)
CARPETA_SALIDA = PROYECTO_RAIZ / "output"
CARPETA_DATOS = CARPETA_SALIDA / "data"

#archivos finales que consumira PowerBI
RUTA_PARQUET_D2 = CARPETA_DATOS / "d2.parquet"
RUTA_PARQUET_PF = CARPETA_DATOS / "pf.parquet"

#base de datos de auditoria (historial)
RUTA_BASE_DATOS = CARPETA_SALIDA / "logs.db"

#--CONFIGURACION DEL NEGOCIO--
VERSIONES_MINERIA = [
    "01 Ejecucion",
    "02 API Original",
    "03 API Control",
    "04 Ver Cero",
    "05 Ver Prima",
    "06 Ver Uno",
    "07 Ver Ctrl Trim",
    "08 Ver PND",
    "09 Ver EAT Ant",
]
#hojas de excel
HOJA_D2 = "D2" 
HOJA_PF = "PF"
#metadatos y validacion
#ID_BLANCO: la clave para el modelo dimensional de PowerBI
COL_VALIDACION = "ID_BLANCO"
#columnas para auditoria
COL_VERSION = "_version_presupuesto"
COL_PROYECTO = "_codigo_proyecto"
COL_ARCHIVO = "_nombre_archivo"
COL_RUTA_ARCHIVO = "_ruta_archivo"
COL_FECHA_MODIF = "_fecha_modificacion"
COL_FECHA_EXTRACCION = "_fecha_extraccion"
 
#molde de columnas
ARCHIVO_BASE = "00Base.xlsx" 