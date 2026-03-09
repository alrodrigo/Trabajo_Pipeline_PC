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
    COL_FECHA_EXTRACCION,
    COL_VALIDACION_PF,
    COLUMNAS_BASE_D2,
    COLUMNAS_BASE_PF

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
            """Verifica que los encabezados del DataFrame sean válidos."""
            total_cols = len(df.columns)
            if total_cols == 0:
                return

            unnamed_count = sum(1 for c in df.columns if str(c).startswith("Unnamed:"))
            ratio = unnamed_count / total_cols

            if ratio > 0.5:
                # MENSAJE AMIGABLE 1: Columnas Fantasma
                raise EstructuraExcelError(
                    f"La hoja '{nombre_hoja}' tiene demasiadas columnas vacías detectadas ({unnamed_count} columnas fantasma). "
                    f"SOLUCIÓN: Abra el Excel, seleccione las columnas en blanco a la derecha de sus datos, elimínelas y guarde."
                )
            
    def _validar_esquema_columnas(self, df: pd.DataFrame, columnas_esperadas: list, nombre_hoja: str, ruta_archivo: Path) -> None:
        """Verifica que no falten columnas oficiales e ignora las columnas de fechas dinámicas."""
        if not columnas_esperadas:
            return
            
        columnas_actuales = set(df.columns)
        esperadas = set(columnas_esperadas)
        
        faltantes = esperadas - columnas_actuales
        sobrantes = columnas_actuales - esperadas
        
        # 1. ERROR CRÍTICO: Falta una columna de la plantilla base
        if faltantes:
                faltantes_lista = list(faltantes)
                
                # Si faltan 5 o menos, le decimos EXACTAMENTE cuáles son
                if len(faltantes_lista) <= 5:
                    faltantes_str = ", ".join(f"'{c}'" for c in faltantes_lista)
                    texto_exacto = f"Faltan exactamente estas columnas: {faltantes_str}."
                # Si faltan muchas, le damos el total y las primeras 5 para no romper la pantalla
                else:
                    faltantes_str = ", ".join(f"'{c}'" for c in faltantes_lista[:5])
                    texto_exacto = f"Faltan {len(faltantes_lista)} columnas obligatorias en total. Las primeras que faltan son: {faltantes_str}..."

                raise EstructuraExcelError(
                    f"La hoja '{nombre_hoja}' no respeta la plantilla oficial. "
                    f"{texto_exacto} "
                    f"SOLUCIÓN: Revise el Excel, corrija estos títulos y vuelva a intentar."
                )
                
        # 2. ADVERTENCIA: Columnas inventadas (Ignorando las fechas)
        if sobrantes:
            # Esta regla detecta formatos como "ene-2024", "feb-2025", o "ene-2024_1" (desduplicados)
            patron_fecha = re.compile(r"^(ene|feb|mar|abr|may|jun|jul|ago|sep|oct|nov|dic)-\d{4}(_\d+)?$")
            
            sobrantes_reales = []
            for c in sobrantes:
                c_str = str(c)
                # Si es columna fantasma, la ignoramos
                if c_str.startswith("Unnamed"): 
                    continue
                # Si es una columna de fecha, es válida, la ignoramos
                if patron_fecha.match(c_str): 
                    continue
                # Si no es ninguna de las dos, es una columna inventada por el usuario
                sobrantes_reales.append(c_str)

            if sobrantes_reales:
                sobrantes_str = ", ".join(sobrantes_reales[:5])
                print(f"  [ADVERTENCIA] '{ruta_archivo.name}' -> La hoja '{nombre_hoja}' tiene {len(sobrantes_reales)} columnas no reconocidas (Ej: {sobrantes_str}). El motor las ignorará.")

    # ---------------------------------------------------------
    # ACTUALIZACIÓN: LECTOR GENERAL (Sirve para D2 y PF)
    # ---------------------------------------------------------
    def _leer_hoja(self, ruta_archivo: Path, nombre_hoja: str, fila_encabezado: int, col_validacion: str, columnas_esperadas: list) -> pd.DataFrame | None:
        """Lee una hoja de excel aplicando todos los escudos de validación."""
        try:
            df = pd.read_excel(ruta_archivo, sheet_name=nombre_hoja, header=fila_encabezado, engine="openpyxl")
        except (ValueError, KeyError):
            return None
        except Exception as e:
            print(f"Error crítico leyendo {nombre_hoja} en '{ruta_archivo.name}': {e}")
            return None

        if df.empty: return None

        self._validar_encabezados(df, ruta_archivo, nombre_hoja)
        df.columns = [self._formatear_columna_fecha(col) for col in df.columns]
        df = self._desduplicar_columnas(df)
        df = df.dropna(how="all")

        if df.empty: return None
        
        # APLICAMOS EL NUEVO ESCUDO DE ESQUEMA
        self._validar_esquema_columnas(df, columnas_esperadas, nombre_hoja, ruta_archivo)

        # EL FILTRO DE LLAVE AHORA ES DINÁMICO (id_blanco para D2, BLANCO para PF)
        if col_validacion not in df.columns:
            raise EstructuraExcelError(
                f"Falta la columna clave '{col_validacion}' en la hoja '{nombre_hoja}'. "
                f"SOLUCIÓN: Verifique que el título esté escrito exactamente así, sin espacios."
            )
        
        # Limpiamos basura
        df = df.dropna(subset=[col_validacion])

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

# ---------------------------------------------------------
    # ACTUALIZACIÓN: ORQUESTADOR
    # ---------------------------------------------------------
    def leer_archivo_proyecto(self, ruta_archivo: Path, version_presupuesto: str, fecha_modificacion: datetime) -> tuple[pd.DataFrame | None, pd.DataFrame | None]:
        """Función principal para leer D2 y PF."""
        if not self.verificar_archivo_disponible(ruta_archivo):
            print(f"Archivo '{ruta_archivo.name}' en uso por otra persona, omitiendo.")
            return None, None

        # Procesar D2 (Le pasamos su columna clave y su lista de 140 columnas esperadas)
        df_d2 = self._leer_hoja(ruta_archivo, HOJA_D2, self._FILA_ENCABEZADO_D2, COL_VALIDACION, COLUMNAS_BASE_D2)
        if df_d2 is not None:
            df_d2 = self.agregar_metadatos(df_d2, ruta_archivo, version_presupuesto, fecha_modificacion)

        # Procesar PF (Le pasamos su nueva columna clave "BLANCO" y su lista esperada)
        df_pf = self._leer_hoja(ruta_archivo, HOJA_PF, self._FILA_ENCABEZADO_PF, COL_VALIDACION_PF, COLUMNAS_BASE_PF)
        if df_pf is not None:
            df_pf = self.agregar_metadatos(df_pf, ruta_archivo, version_presupuesto, fecha_modificacion)

        return df_d2, df_pf
