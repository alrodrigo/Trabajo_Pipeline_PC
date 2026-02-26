"""
Módulo de gestión de historial y auditoría (SQLite).
Incorpora manejo de hilos y reintentos ante fallos de red.
"""

import sqlite3
import getpass
import time
import threading
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional

from src.ajustes import RUTA_BASE_DATOS

class GestorHistorial:
    def __init__(self):
        self.db_path = RUTA_BASE_DATOS
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._inicializar_tablas()

    def _conectar(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=20)
        conn.execute("PRAGMA journal_mode=WAL") 
        conn.execute("PRAGMA foreign_keys=ON")
        conn.row_factory = sqlite3.Row
        return conn

    def _ejecutar_con_reintento(self, operacion_sql, parametros=(), devolver_id=False):
        """Ejecuta SQL. Si devolver_id es True, asegura extraer el ID insertado."""
        intentos = 3
        ultimo_error = None
        
        with self._lock:
            for i in range(intentos):
                conn = None
                try:
                    conn = self._conectar()
                    cursor = conn.cursor()
                    cursor.execute(operacion_sql, parametros)
                    conn.commit()
                    
                    if devolver_id:
                        res_id = cursor.lastrowid
                        # FALLBACK SEGURO: Si lastrowid falla, consultamos el máximo ID directamente
                        if res_id is None or res_id == 0:
                            res_id = conn.execute("SELECT MAX(id) FROM ejecuciones").fetchone()[0]
                        return res_id
                        
                    return True
                except sqlite3.OperationalError as e:
                    ultimo_error = e
                    time.sleep(0.5 * (i + 1))
                finally:
                    if conn:
                        conn.close()
        raise ultimo_error

    def _inicializar_tablas(self):
        sql_script = """
            CREATE TABLE IF NOT EXISTS ejecuciones (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                fecha_inicio      DATETIME NOT NULL,
                fecha_fin         DATETIME,
                usuario           TEXT NOT NULL,
                estado            TEXT NOT NULL DEFAULT 'en_curso',
                total_archivos    INTEGER DEFAULT 0,
                archivos_exitosos INTEGER DEFAULT 0,
                archivos_error    INTEGER DEFAULT 0,
                duracion_segundos REAL
            );

            CREATE TABLE IF NOT EXISTS archivos_procesados (
                id                 INTEGER PRIMARY KEY AUTOINCREMENT,
                ejecucion_id       INTEGER NOT NULL,
                ruta_archivo       TEXT NOT NULL,
                nombre_archivo     TEXT NOT NULL,
                carpeta_version    TEXT NOT NULL,
                fecha_modificacion DATETIME,
                fecha_lectura      DATETIME NOT NULL,
                estado             TEXT NOT NULL,
                mensaje_error      TEXT,
                filas_d2           INTEGER DEFAULT 0,
                filas_pf           INTEGER DEFAULT 0,
                tamano_bytes       INTEGER DEFAULT 0,
                FOREIGN KEY (ejecucion_id) REFERENCES ejecuciones(id)
            );
            
            CREATE INDEX IF NOT EXISTS idx_ruta_estado ON archivos_procesados(ruta_archivo, estado);
        """
        with self._lock:
            conn = self._conectar()
            conn.executescript(sql_script)
            conn.close()

    def registrar_inicio_ejecucion(self) -> int:
        id_generado = self._ejecutar_con_reintento(
            "INSERT INTO ejecuciones (fecha_inicio, usuario, estado) VALUES (?, ?, ?)",
            (datetime.now().isoformat(), getpass.getuser(), "en_curso"),
            devolver_id=True  # <--- Pedimos que devuelva el ID forzosamente
        )
        if id_generado is None:
            raise Exception("La base de datos no generó un ID de ejecución válido.")
        return id_generado

    def finalizar_ejecucion(self, ejecucion_id: int, estado: str, stats: Dict[str, int], duracion: float):
        self._ejecutar_con_reintento(
            """UPDATE ejecuciones
               SET fecha_fin = ?, estado = ?, total_archivos = ?,
                   archivos_exitosos = ?, archivos_error = ?, duracion_segundos = ?
               WHERE id = ?""",
            (
                datetime.now().isoformat(),
                estado,
                stats.get('total', 0),
                stats.get('exitosos', 0),
                stats.get('errores', 0),
                duracion,
                ejecucion_id,
            )
        )

    def verificar_si_procesar(self, ruta_archivo: Path, fecha_modif_actual: datetime) -> bool:
        ruta_str = str(ruta_archivo)
        try:
            with self._lock:
                conn = self._conectar()
                row = conn.execute(
                    """SELECT fecha_modificacion FROM archivos_procesados
                       WHERE ruta_archivo = ? AND estado = 'EXITO'
                       ORDER BY fecha_lectura DESC LIMIT 1""",
                    (ruta_str,),
                ).fetchone()
                conn.close()

            if row is None or row['fecha_modificacion'] is None:
                return True

            ultima_fecha = datetime.fromisoformat(row['fecha_modificacion'])
            return fecha_modif_actual > ultima_fecha
        except Exception as e:
            return True

    def registrar_archivo(self, ejecucion_id: int, ruta: Path, version: str, 
                          f_modif: datetime, estado: str, 
                          filas_d2: int = 0, filas_pf: int = 0, error: str = None,
                          tamano_bytes: int = 0):
        if ejecucion_id is None:
            print(f"AVISO: ID Ejecución es NULO al guardar {ruta.name}")
            return

        if tamano_bytes == 0:
            try:
                tamano_bytes = ruta.stat().st_size if ruta.exists() else 0
            except:
                pass

        self._ejecutar_con_reintento(
            """INSERT INTO archivos_procesados
               (ejecucion_id, ruta_archivo, nombre_archivo, carpeta_version,
                fecha_modificacion, fecha_lectura, estado, mensaje_error,
                filas_d2, filas_pf, tamano_bytes)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                ejecucion_id,
                str(ruta),
                ruta.name,
                version,
                f_modif.isoformat(),
                datetime.now().isoformat(),
                estado,
                error,
                filas_d2,
                filas_pf,
                tamano_bytes,
            )
        )
        
    def obtener_historial_reciente(self, limite: int = 50) -> List[dict]:
        sql = """SELECT id, fecha_inicio, fecha_fin, usuario, estado, 
                        total_archivos, archivos_exitosos, archivos_error, duracion_segundos
                 FROM ejecuciones ORDER BY id DESC LIMIT ?"""
        try:
            with self._lock:
                conn = self._conectar()
                filas = conn.execute(sql, (limite,)).fetchall()
                conn.close()
            return [dict(f) for f in filas]
        except Exception:
            return []

    def obtener_detalles_archivo(self, ejecucion_id: int) -> List[dict]:
        sql = """SELECT nombre_archivo, carpeta_version, estado, filas_d2, filas_pf, mensaje_error
                 FROM archivos_procesados WHERE ejecucion_id = ? ORDER BY id ASC"""
        try:
            with self._lock:
                conn = self._conectar()
                filas = conn.execute(sql, (ejecucion_id,)).fetchall()
                conn.close()
            return [dict(f) for f in filas]
        except Exception:
            return []