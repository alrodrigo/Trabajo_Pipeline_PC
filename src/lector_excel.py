import re
import os
import pandas as pd
from datetime import datetime
from pathlib import Path
from pandas import Timestamp
from src.ajustes import (
    HOJA_D2,
    HOJA_PF,
    COL_VALIDACION,
    COL_VERSION,
    COL_PROYECTO,
    COL_ARCHIVO,
    COL_RUTA_ARCHIVO,
    COL_FECHA_MODIF,
    COL_FECHA_EXTRACCION
)
_MESES_ES = {
    1: "ene", 2: "feb", 3: "mar", 4: "abr", 5: "may", 6: "jun",
    7: "jul", 8: "ago", 9: "sep", 10: "oct", 11: "nov", 12: "dic",
}
class EstructuraExcelError(Exception):
    """Error personalizado para fallos de estructura en los archivos de la mineria"""
    pass
class LectorExcel:
    def __init__(self):
        # Fila de encabezado en la hoja D2 (las filas 0-2 son títulos/logos)
        self._FILA_ENCABEZADO_D2 = 3
        # Fila de encabezado en la hoja PF (suele empezar en la fila 0)
        self._FILA_ENCABEZADO_PF = 0

    def verificar_archivo_disponible(self, ruta_archivo: Path) -> bool:
        """Verifica si el archivo no está bloqueado por el usuario."""
        try:
            if ruta_archivo.exists():
                os.rename(ruta_archivo, ruta_archivo)
                return True
            return False
        except OSError:
            return False

    def extraer_codigo_proyecto(self, nombre_archivo: str) -> str:
        """Extrae el código de proyecto del nombre del archivo."""
        match = re.match(r"^(\S+)", nombre_archivo)
        return match.group(1) if match else nombre_archivo

    def _formatear_columna_fecha(self, col):
        """Normaliza nombres de columna tipo fecha a 'ene-2024'."""
        if isinstance(col, (Timestamp, datetime)):
            return f"{_MESES_ES[col.month]}-{col.year}"
        if isinstance(col, str):
            try:
                dt = datetime.strptime(col, "%Y-%m-%d %H:%M:%S")
                return f"{_MESES_ES[dt.month]}-{dt.year}"
            except ValueError:
                return col
        return col

    def _desduplicar_columnas(self, df: pd.DataFrame) -> pd.DataFrame:
        """Renombra columnas duplicadas con sufijos numéricos."""
        columnas = list(df.columns)
        conteo = {}
        nuevas = []
        for col in columnas:
            col_str = str(col)
            if col_str in conteo:
                conteo[col_str] += 1
                nuevas.append(f"{col_str}_{conteo[col_str]}")
            else:
                conteo[col_str] = 0
                nuevas.append(col_str)
        df.columns = nuevas
        return df

    def _validar_encabezados(self, df: pd.DataFrame, ruta_archivo: Path, nombre_hoja: str) -> None:
        """Detecta si la fila de encabezado es incorrecta."""
        total_cols = len(df.columns)
        if total_cols == 0:
            return
        unnamed_count = sum(1 for c in df.columns if str(c).startswith("Unnamed"))
        ratio = unnamed_count / total_cols
        if ratio > 0.5:
            raise EstructuraExcelError(
                f"El archivo '{ruta_archivo.name}', en la hoja '{nombre_hoja}', "
                f"tiene {unnamed_count} columnas sin nombre. Revise el formato."
            )

    def _leer_hoja(self, ruta_archivo: Path, nombre_hoja: str, fila_encabezado: int) -> pd.DataFrame | None:
        """Lee una hoja de excel, limpia encabezados y filtra por ID_BLANCO."""
        try:
            df = pd.read_excel(
                ruta_archivo,
                sheet_name=nombre_hoja,
                header=fila_encabezado,
                engine="openpyxl",
            )
        except (ValueError, KeyError):
            return None
        except Exception as e:
            print(f"Error leyendo {nombre_hoja} en '{ruta_archivo.name}': {e}")
            return None

        if df.empty:
            return None

        self._validar_encabezados(df, ruta_archivo, nombre_hoja)
        df.columns = [self._formatear_columna_fecha(col) for col in df.columns]
        df = self._desduplicar_columnas(df)
        df = df.dropna(how="all")

        if df.empty:
            return None

        # --- FILTRO CRÍTICO SOLICITADO ---
        if COL_VALIDACION in df.columns:
            df = df.dropna(subset=[COL_VALIDACION])
        elif nombre_hoja == HOJA_D2:
            raise EstructuraExcelError(
                f"La hoja '{nombre_hoja}' de '{ruta_archivo.name}' no tiene la columna '{COL_VALIDACION}'."
            )

        return df if not df.empty else None
    def agregar_metadatos(self, df: pd.DataFrame, ruta_archivo: Path, version_presupuesto: str, fecha_modificacion: datetime) -> pd.DataFrame:
        """Agrega columnas de auditoría optimizando la memoria de Pandas."""
        nombre_archivo = ruta_archivo.name
        
        # Creamos un bloque con todas las columnas nuevas de golpe
        nuevas_columnas = pd.DataFrame({
            COL_VERSION: version_presupuesto,
            COL_PROYECTO: self.extraer_codigo_proyecto(nombre_archivo),
            COL_ARCHIVO: nombre_archivo,
            COL_RUTA_ARCHIVO: str(ruta_archivo),
            COL_FECHA_MODIF: fecha_modificacion.isoformat(),
            COL_FECHA_EXTRACCION: datetime.now().isoformat()
        }, index=df.index)
        
        # Las pegamos al DataFrame original en una sola operación (cero fragmentación)
        return pd.concat([df, nuevas_columnas], axis=1)

    def leer_archivo_proyecto(self, ruta_archivo: Path, version_presupuesto: str, fecha_modificacion: datetime) -> tuple[pd.DataFrame | None, pd.DataFrame | None]:
        """Función principal para leer D2 y PF."""
        if not self.verificar_archivo_disponible(ruta_archivo):
            print(f"Archivo '{ruta_archivo.name}' en uso, omitiendo.")
            return None, None

        # Procesar D2
        try:
            df_d2 = self._leer_hoja(ruta_archivo, HOJA_D2, self._FILA_ENCABEZADO_D2)
            if df_d2 is not None:
                df_d2 = self.agregar_metadatos(df_d2, ruta_archivo, version_presupuesto, fecha_modificacion)
        except EstructuraExcelError as e:
            print(f"Error en D2: {e}")
            df_d2 = None

        # Procesar PF
        try:
            df_pf = self._leer_hoja(ruta_archivo, HOJA_PF, self._FILA_ENCABEZADO_PF)
            if df_pf is not None:
                df_pf = self.agregar_metadatos(df_pf, ruta_archivo, version_presupuesto, fecha_modificacion)
        except EstructuraExcelError as e:
            print(f"Error en PF: {e}")
            df_pf = None

        return df_d2, df_pf
