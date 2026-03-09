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
        """
        Toma una columna de Excel (Date o String) y la convierte en 'AAAA-MM-DD'
        asegurando que sea el ÚLTIMO día de ese mes (Estándar Codelco).
        """
        target_date = None
        #caso a: pandas ya lo detecto como fecha / timestamp
        if isinstance(col, (Timestamp, datetime)):
            target_date = pd.Timestamp(col)
        #caso b: viene como string (ej: "2024-01-31" 00:00:00)
        elif isinstance(col, str):
            try:
                #intentamos parsear el formato datetime estandar
                target_date = pd.to_datetime(col, format="%Y-%m-%d %H:%M:%S")
            except ValueError:
                return col  # Si no es parseable, lo dejamos como está (podría ser un título de columna normal)
        if target_date:
            #pandas: 'monthEnd(0)' garantiza que vamos al final del mes
            #si ya es el dia final (ej:31), no cambia nada; si es un dia intermedio (ej: 2024-01-15), lo ajusta al final del mes (2024-01-31)
            last_day = target_date + pd.offsets.MonthEnd(0)
            return last_day.strftime("%Y-%m-%d") #devolvemos AAAA-MM-DD como string para evitar problemas de tipo en parquet
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
           # Nuevo Regex estricto para AAAA-MM-DD
            patron_fecha = re.compile(r"^\d{4}-\d{2}-\d{2}(_\d+)?$")  # Permite también sufijos de desduplicación como _1, _2, etc.
            
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

    def _sanitizar_para_parquet(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Escudo final: Convierte tipos de datos conflictivos (como fechas y tiempos mezclados) 
        a texto ISO 8601 para que PyArrow (Parquet) los guarde automáticamente sin explotar.
        """
        for col in df.columns:
            # 1. Si detectamos una columna pura de fechas, forzamos el formato ISO AAAA-MM-DD
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = df[col].dt.strftime('%Y-%m-%d')
                
            # 2. Si la columna es "mixta" (texto, números y fechas mezcladas)
            elif df[col].dtype == 'object':
                # Convertimos las fechas ocultas a formato ISO AAAA-MM-DD
                df[col] = df[col].apply(
                    lambda x: x.strftime('%Y-%m-%d') if isinstance(x, (datetime, Timestamp)) 
                    else x
                )
                # Blindaje PyArrow: Forzamos toda la basura mixta a texto, 
                # devolviendo los "vacíos" a estado nulo para la base de datos.
                df[col] = df[col].astype(str).replace({'nan': None, 'NaT': None, 'None': None, '<NA>': None})
                
        return df

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
        #1. Limpieza base de filas vacias antes de formatear titulos
        df = df.dropna(how="all")
    
        self._validar_encabezados(df, ruta_archivo, nombre_hoja)
        #2. formateo de titulos y estandarizacion ISO
        df.columns = [self._formatear_columna_fecha(col) for col in df.columns]
        df = self._desduplicar_columnas(df)
        #3. validacion de esquema
        self._validar_esquema_columnas(df, columnas_esperadas, nombre_hoja, ruta_archivo)
        #4. Slicing extremo: identificar solo lo que su queremos guardar
        # 4. SLICING EXTREMO Y ORDENAMIENTO PERFECTO
        # 4.1. Rescatamos las columnas base manteniendo su orden original estricto
        cols_base_ordenadas = [c for c in columnas_esperadas if c in df.columns]
        
        # 4.2. Detectamos las fechas ISO y las ordenamos cronológicamente (de menor a mayor)
        patron_fecha_iso = re.compile(r"^\d{4}-\d{2}-\d{2}(_\d+)?$")
        cols_fechas_iso = [c for c in df.columns if patron_fecha_iso.match(str(c))]
        cols_fechas_iso.sort() # Esto garantiza que 2024 aparezca antes que 2025
        
        # 4.3. Unimos las listas: Primero los datos estáticos, al final la línea de tiempo
        cols_finales_ordenadas = cols_base_ordenadas + cols_fechas_iso
        
        # Recortamos el DataFrame aplicando el nuevo orden
        df = df[cols_finales_ordenadas]
        #5. filtro de llave principal (filtro nulls)
        if col_validacion not in df.columns:
            raise EstructuraExcelError(
                f"Falta columna clave '{col_validacion}' en la hoja '{nombre_hoja}'. "
                f"SOLUCIÓN: Verifique que el título esté escrito exactamente así, sin espacios."
            )
        df = df.dropna(subset=[col_validacion])
        df[col_validacion] = df[col_validacion].astype(str).str.strip()
        #si la columna clave existe pero esta vacia, pandas lo carga como null
        #lo convertimos a string para asegurar consistencia de parquet
        df = self._sanitizar_para_parquet(df)
        return df if not df.empty else None
        

    def agregar_metadatos(self, df: pd.DataFrame, ruta_archivo: Path, version_presupuesto: str, fecha_modificacion: datetime) -> pd.DataFrame:
        """Agrega columnas de auditoría optimizando la fragmentación de memoria."""
        nombre_archivo = ruta_archivo.name
        
        nuevas_columnas = pd.DataFrame({
            COL_VERSION: version_presupuesto,
            COL_PROYECTO: self.extraer_codigo_proyecto(nombre_archivo),
            COL_ARCHIVO: nombre_archivo,
            COL_RUTA_ARCHIVO: str(ruta_archivo),
            COL_FECHA_MODIF: pd.Timestamp(fecha_modificacion).isoformat(),
            COL_FECHA_EXTRACCION: pd.Timestamp.now().isoformat()
        }, index=df.index)
        
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
##comentario de prueba 