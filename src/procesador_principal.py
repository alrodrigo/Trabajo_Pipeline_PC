import time
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional
"""orquestador principal del proceso de lectura, el historial y la escritura final de datos
Refactorizado a orientacion a objetos para mayor rebustes"""
from src.ajustes import (VERSIONES_MINERIA, HOJA_D2, HOJA_PF, RUTA_PARQUET_D2, RUTA_PARQUET_PF
                         , ARCHIVO_BASE)
from src.lector_excel import LectorExcel, EstructuraExcelError
from src.escritor_parquet import EscritorParquet
from src.gestor_historial import GestorHistorial
class ProcesadorPrincipal:
    def __init__(self, ruta_base: Path, forzar: bool = False, on_progreso: Optional[Callable] = None):
        #args: ruta_base: ruta raiz donde estan las carpetas de presupuesto, forzar: si es True, procesa todo aunque ya haya sido procesado antes, on_progreso: callback para reportar progreso a la GUI
        self.ruta_base = ruta_base
        self.forzar = forzar
        self.on_progreso = on_progreso
        
        #instanciamos los trabajadores
        self.lector = LectorExcel()
        self.escritor = EscritorParquet()
        self.historial = GestorHistorial()
    
    def _notificar(self, evento: dict):
        """envia un evento de progreso a la GUI, si se proporciono un callback."""
        if self.on_progreso:
            self.on_progreso(evento)


    def _contar_archivos_totales(self) -> int:
        """cuenta cuantos archivos xlsx validos existen para calcular la barra de progreso."""
        total = 0
        for version in VERSIONES_MINERIA:
            ruta_carpeta = self.ruta_base / version
            if ruta_carpeta.exists():
                #filtramos temporales (~$) y el archivo base
                archivos = [f for f in ruta_carpeta.glob("*.xlsx")
                            if f.name != ARCHIVO_BASE and not f.name.startswith("~$")]
                total += len(archivos)
        return total
    
    def ejecutar(self) -> dict:
        """Función maestra que corre todo el proceso."""
        inicio_tiempo = time.time()
        if self.forzar:
            print("\n🧹 MODO FORZADO: Destruyendo Parquets antiguos por seguridad...")
            # RUTA_PARQUET_D2.parent nos da la carpeta exacta donde viven tus Parquets
            carpeta_salida = RUTA_PARQUET_D2.parent 
            self.escritor.limpiar_outputs_antiguos(carpeta_salida)
        # 1. Registrar inicio en BD
        id_ejecucion = self.historial.registrar_inicio_ejecucion()
        
        # 2. Preparar contadores
        total_esperado = self._contar_archivos_totales()
        self._notificar({"tipo": "inicio", "total_archivos": total_esperado})
        
        stats = {
            "total_procesados": 0,
            "exitosos": 0,
            "errores": 0,
            "omitidos": 0,
            "sin_datos": 0
        }
        
        # Bolsas para acumular dataframes antes de guardar
        bolsa_d2 = []
        bolsa_pf = []

        # =======================================================
        # BUCLE 1: RECORREMOS LAS CARPETAS (01 Ejecucion, 02 API...)
        # =======================================================
        for version in VERSIONES_MINERIA:
            ruta_carpeta = self.ruta_base / version
            if not ruta_carpeta.exists():
                continue

            archivos = sorted([f for f in ruta_carpeta.glob("*.xlsx") 
                             if f.name != ARCHIVO_BASE and not f.name.startswith("~$")])

            # =======================================================
            # BUCLE 2: RECORREMOS LOS EXCEL DENTRO DE LA CARPETA
            # =======================================================
            for ruta_archivo in archivos:
                stats["total_procesados"] += 1
                
                try:
                    f_mod = datetime.fromtimestamp(ruta_archivo.stat().st_mtime)
                    tamano = ruta_archivo.stat().st_size
                except OSError:
                    msg = "No se pudo acceder al archivo"
                    self.historial.registrar_archivo(id_ejecucion, ruta_archivo, version, datetime.now(), "ERROR", error=msg)
                    stats["errores"] += 1
                    self._notificar({"tipo": "archivo_error", "nombre": ruta_archivo.name, "error": msg})
                    continue

                self._notificar({
                    "tipo": "archivo_inicio",
                    "nombre": ruta_archivo.name,
                    "indice": stats["total_procesados"],
                    "total": total_esperado
                })

                if not self.forzar and not self.historial.verificar_si_procesar(ruta_archivo, f_mod):
                    stats["omitidos"] += 1
                    self.historial.registrar_archivo(
                        id_ejecucion, ruta_archivo, version, f_mod, "OMITIDO", tamano_bytes=tamano
                    )
                    self._notificar({"tipo": "archivo_omitido", "nombre": ruta_archivo.name})
                    continue

                try:
                    df_d2, df_pf = self.lector.leer_archivo_proyecto(ruta_archivo, version, f_mod)
                    
                    filas_d2 = len(df_d2) if df_d2 is not None else 0
                    filas_pf = len(df_pf) if df_pf is not None else 0

                    if df_d2 is not None: bolsa_d2.append(df_d2)
                    if df_pf is not None: bolsa_pf.append(df_pf)

                    if filas_d2 == 0 and filas_pf == 0:
                        estado = "SIN_DATOS"
                        stats["sin_datos"] += 1
                        self._notificar({"tipo": "archivo_sin_datos", "nombre": ruta_archivo.name})
                    else:
                        estado = "EXITO"
                        stats["exitosos"] += 1
                        self._notificar({
                            "tipo": "archivo_ok", 
                            "nombre": ruta_archivo.name,
                            "filas_d2": filas_d2,
                            "filas_pf": filas_pf
                        })

                    self.historial.registrar_archivo(
                        id_ejecucion, ruta_archivo, version, f_mod, estado, 
                        filas_d2=filas_d2, filas_pf=filas_pf, tamano_bytes=tamano
                    )
                    print(f"  [{estado}] {ruta_archivo.name}")

                except EstructuraExcelError as e:
                    stats["errores"] += 1
                    self.historial.registrar_archivo(id_ejecucion, ruta_archivo, version, f_mod, "ERROR", error=str(e))
                    self._notificar({"tipo": "archivo_error", "nombre": ruta_archivo.name, "error": str(e)})
                    print(f"  [ESTRUCTURA] {ruta_archivo.name}: {e}")
                
                except Exception as e:
                    stats["errores"] += 1
                    self.historial.registrar_archivo(id_ejecucion, ruta_archivo, version, f_mod, "ERROR", error=str(e))
                    self._notificar({"tipo": "archivo_error", "nombre": ruta_archivo.name, "error": str(e)})
                    print(f"  [ERROR] {ruta_archivo.name}: {e}")

        # =======================================================
        # ESTO ESTÁ AFUERA DE LOS BUCLES (Corre al terminar todo)
        # =======================================================
        print("\n--- Consolidando Parquet ---")
        
        res_d2 = self.escritor.consolidar_y_guardar(bolsa_d2, RUTA_PARQUET_D2)
        if hasattr(self, '_notificar_parquet'):
            self._notificar_parquet("D2", res_d2)
        
        res_pf = self.escritor.consolidar_y_guardar(bolsa_pf, RUTA_PARQUET_PF)
        if hasattr(self, '_notificar_parquet'):
            self._notificar_parquet("PF", res_pf)

        # 5. Cierre
        duracion = time.time() - inicio_tiempo
        estado_global = "exito"
        if stats["errores"] > 0:
            estado_global = "parcial" if stats["exitosos"] > 0 else "error"

        stats_db = {
            "total": stats["total_procesados"],
            "exitosos": stats["exitosos"],
            "errores": stats["errores"]
        }
        
        self.historial.finalizar_ejecucion(id_ejecucion, estado_global, stats_db, duracion)
        
        resultado_final = {
            "estado": estado_global,
            "stats": stats,
            "duracion": round(duracion, 2)
        }
        self._notificar({"tipo": "fin", "resultado": resultado_final})
        
        return resultado_final
    def _notificar_parquet(self, tipo_hoja, resultado):
        """auxiliar para enviar notificacion de escritura de parquet."""
        self._notificar({
            "tipo": "parquet_escrito",
            "tipo_hoja": tipo_hoja,
            "filas": resultado.filas_totales,
            "nuevas": resultado.filas_nuevas,
            "fusion": resultado.fusion_realizada
        })
        print(f" {tipo_hoja}.parquet: {resultado.filas_totales} filas.")
        