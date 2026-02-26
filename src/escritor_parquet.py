import logging
import numpy as np
import pandas as pd
from dataclasses import dataclass
from pathlib import Path
from src.ajustes import COL_RUTA_ARCHIVO

#configuramos un logger para errores de escritura
logger = logging.getLogger(__name__)
@dataclass
class ResultadoEscritura:
    """clase para reportar estadisticas de la grabacion a la GUI."""
    filas_totales: int
    filas_nuevas: int
    filas_preservadas: int
    fusion_realizada: bool

class EscritorParquet:
    def __init__(self):
        self.engine = "pyarrow"
        self.compression = "snappy"
    
    def _normalizar_tipos(self, df: pd.DataFrame) -> pd.DataFrame:
        """Evita el error 'mixed types' de parquet.
        si una columna tiene numeros y letras (comun en Excel), lo convierte todo a texto."""
        for col in df.columns:
            #si la columna es de tipo object (mezcla), normalizamos a string
            if df[col].dtype == object:
                df[col] = df[col].astype(str).replace(["None", "nan", "NaN"], np.nan)
        return df
    def guardar(self, df: pd.DataFrame, ruta_salida: Path):
        """graba un dataframe directamente a parquet."""
        ruta_salida.parent.mkdir(parents=True, exist_ok=True)
        df = self._normalizar_tipos(df)
        df.to_parquet(ruta_salida, engine=self.engine, compression=self.compression, index=False)
    
    def consolidar_y_guardar(self, lista_dataframes: list, ruta_salida: Path) -> ResultadoEscritura:
        """
        toma los datos nuevos, los mezcla con el parquet que ya existia en disco
        y reemplaza solo lo necesario."""
        #si no hay nada nuevo que escribir, salimos
        if not lista_dataframes:
            return ResultadoEscritura(0, 0, 0, False)
        #2. Unimos todos los excels nuevos en un solo bloque
        df_nuevos = pd.concat(lista_dataframes, ignore_index=True)
        filas_nuevas = len(df_nuevos)
        filas_preservadas = 0
        fusion_realizada = False
        #3. ya existe un archivo de dias anteriores?
        if ruta_salida.exists():
            try:
                df_existente = pd.read_parquet(ruta_salida, engine=self.engine)
                #incremental
                #indentificamos que archivos estamos procesando hoy
                rutas_hoy = set(df_nuevos[COL_RUTA_ARCHIVO].unique())
                # del archivo viejo, nos quedamos solo con lo que no estamos procesando hoy
                #el simbolo ~ es negacion, asi que esto es "filtra el df existente para quedarte solo con las filas cuyo COL_RUTA_ARCHIVO no esta en rutas_hoy"
                df_preservado = df_existente[~df_existente[COL_RUTA_ARCHIVO].isin(rutas_hoy)]
                filas_preservadas = len(df_preservado)
                #unimos lo nuevo con lo preservado
                df_consolidado = pd.concat([df_preservado, df_nuevos], ignore_index=True)
                fusion_realizada = filas_preservadas > 0
            except Exception as e:
                logger.warning(f"No se pudo fusionar Parquet existente {ruta_salida.name}: {e}")
                df_consolidado = df_nuevos
        else: #si no existe, simplemente guardamos lo nuevo
            df_consolidado = df_nuevos
        #4. Guardamos el bloque consolidado, reemplazando lo que habia antes
        self.guardar(df_consolidado, ruta_salida)
        return ResultadoEscritura(
            filas_totales=len(df_consolidado),
            filas_nuevas=filas_nuevas,
            filas_preservadas=filas_preservadas,
            fusion_realizada=fusion_realizada
        )