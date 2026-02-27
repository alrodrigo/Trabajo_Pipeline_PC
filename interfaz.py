"""
Interfaz gráfica moderna (CustomTkinter) para el pipeline P&C.
Conectada al nuevo motor robusto de procesamiento y base de datos.
"""

import threading
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from datetime import datetime
from pathlib import Path
import customtkinter as ctk

# Importamos nuestro motor y configuración
from src.ajustes import FILESERVER_LOCAL, FILESERVER_PRODUCCION
from src.procesador_principal import ProcesadorPrincipal
from src.gestor_historial import GestorHistorial

# Configuración visual base de CustomTkinter
ctk.set_appearance_mode("Dark")  # Modos: "System" (estándar), "Dark", "Light"
ctk.set_default_color_theme("blue")  # Temas: "blue" (estándar), "green", "dark-blue"


class VentanaPipeline(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("Pipeline P&C — Codelco (Edición Moderna)")
        self.geometry("900x680")
        self.minsize(800, 600)

        # Variables de estado
        self._ejecutando = False
        self.historial_db = GestorHistorial()

        self._configurar_estilo_tablas()
        self._crear_interfaz()
        self._cargar_historial()

        # Confirmación al cerrar
        self.protocol("WM_DELETE_WINDOW", self._al_cerrar_ventana)

    def _configurar_estilo_tablas(self):
        """CustomTkinter no tiene tablas nativas, así que adaptamos ttk.Treeview al modo oscuro."""
        style = ttk.Style(self)
        style.theme_use("default")
        style.configure("Treeview", 
                        background="#2b2b2b", foreground="white", 
                        fieldbackground="#2b2b2b", borderwidth=0, font=("Segoe UI", 10))
        style.map("Treeview", background=[("selected", "#1f538d")])
        style.configure("Treeview.Heading", 
                        background="#565b5e", foreground="white", 
                        relief="flat", font=("Segoe UI", 10, "bold"))

    def _crear_interfaz(self):
        # --- Pestañas ---
        self.tabview = ctk.CTkTabview(self, corner_radius=10)
        self.tabview.pack(fill=tk.BOTH, expand=True, padx=15, pady=15)

        self.tab_ejecutar = self.tabview.add("▶ Ejecutar Pipeline")
        self.tab_historial = self.tabview.add("⏱ Historial de Ejecuciones")

        self._crear_tab_ejecutar()
        self._crear_tab_historial()

    # ==================================================================
    # PESTAÑA 1: EJECUTAR
    # ==================================================================
    def _crear_tab_ejecutar(self):
        frame = self.tab_ejecutar

        # --- Controles ---
        frame_config = ctk.CTkFrame(frame, corner_radius=10)
        frame_config.pack(fill=tk.X, padx=10, pady=(10, 5))

        # Fila 1: Ruta
        lbl_ruta = ctk.CTkLabel(frame_config, text="Ruta de datos:", font=("Segoe UI", 12, "bold"))
        lbl_ruta.grid(row=0, column=0, padx=15, pady=(15, 5), sticky="w")

        self.var_ruta = tk.StringVar(value=str(FILESERVER_LOCAL))
        entry_ruta = ctk.CTkEntry(frame_config, textvariable=self.var_ruta, width=500)
        entry_ruta.grid(row=0, column=1, padx=5, pady=(15, 5), sticky="we")

        btn_examinar = ctk.CTkButton(frame_config, text="Examinar...", width=100, command=self._seleccionar_carpeta, fg_color="#4a4a4a", hover_color="#333333")
        btn_examinar.grid(row=0, column=2, padx=15, pady=(15, 5))

        # Fila 2: Botones rápidos
        frame_botones_ruta = ctk.CTkFrame(frame_config, fg_color="transparent")
        frame_botones_ruta.grid(row=1, column=1, sticky="w", padx=5)

        ctk.CTkButton(frame_botones_ruta, text="Ruta Local", width=120, height=24, fg_color="#2b2b2b", hover_color="#4a4a4a", command=lambda: self.var_ruta.set(str(FILESERVER_LOCAL))).pack(side=tk.LEFT, padx=(0, 10))
        ctk.CTkButton(frame_botones_ruta, text="Ruta Producción", width=120, height=24, fg_color="#2b2b2b", hover_color="#4a4a4a", command=lambda: self.var_ruta.set(str(FILESERVER_PRODUCCION))).pack(side=tk.LEFT)

        # Fila 3: Checkbox Forzar
        self.var_forzar = tk.BooleanVar(value=False)
        chk_forzar = ctk.CTkCheckBox(frame_config, text="Forzar reprocesamiento (ignorar fechas de modificación)", variable=self.var_forzar)
        chk_forzar.grid(row=2, column=1, sticky="w", padx=5, pady=(15, 15))

        # --- Botón de Ejecución (Grande y llamativo) ---
        self.btn_ejecutar = ctk.CTkButton(frame, text="INICIAR EXTRACCIÓN P&C", height=40, font=("Segoe UI", 14, "bold"), fg_color="#28a745", hover_color="#218838", command=self._iniciar_ejecucion)
        self.btn_ejecutar.pack(pady=15)

        # --- Progreso ---
        self.barra_progreso = ctk.CTkProgressBar(frame, height=12, corner_radius=5)
        self.barra_progreso.pack(fill=tk.X, padx=20)
        self.barra_progreso.set(0)

        self.lbl_estado = ctk.CTkLabel(frame, text="Esperando instrucciones...", text_color="gray")
        self.lbl_estado.pack(anchor="w", padx=20, pady=5)

        # --- Consola Log ---
        self.txt_log = ctk.CTkTextbox(frame, font=("Consolas", 12), fg_color="#0d0d0d", text_color="#cccccc", corner_radius=10)
        self.txt_log.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        self.txt_log.configure(state=tk.DISABLED)

        # Etiquetas de color del código original
        self.txt_log.tag_config("ok", foreground="#4ec94e")
        self.txt_log.tag_config("error", foreground="#e74c3c")
        self.txt_log.tag_config("omitido", foreground="#888888")
        self.txt_log.tag_config("info", foreground="#5dade2")
        self.txt_log.tag_config("exito_resumen", foreground="#2ecc71")

    # ==================================================================
    # PESTAÑA 2: HISTORIAL
    # ==================================================================
    def _crear_tab_historial(self):
        frame = self.tab_historial

        # Botón actualizar
        ctk.CTkButton(frame, text="↻ Actualizar Base de Datos", width=200, fg_color="#1f538d", command=self._cargar_historial).pack(pady=10, anchor="w", padx=10)

        # Tabla Ejecuciones
        frame_ejec = ctk.CTkFrame(frame)
        frame_ejec.pack(fill=tk.X, padx=10, pady=5)
        
        cols = ("ID", "Fecha", "Estado", "OK", "Errores", "Duración")
        self.tree_ejec = ttk.Treeview(frame_ejec, columns=cols, show="headings", height=8)
        anchos = {"ID": 50, "Fecha": 150, "Estado": 100, "OK": 80, "Errores": 80, "Duración": 100}
        
        for c in cols:
            self.tree_ejec.heading(c, text=c)
            self.tree_ejec.column(c, width=anchos[c], anchor=tk.CENTER)
            
        self.tree_ejec.pack(fill=tk.X, padx=5, pady=5)
        self.tree_ejec.bind("<<TreeviewSelect>>", self._al_seleccionar_ejecucion)

        # Tabla Detalle
        lbl_det = ctk.CTkLabel(frame, text="Detalles de los archivos procesados (Seleccione arriba):", font=("Segoe UI", 12, "bold"))
        lbl_det.pack(anchor="w", padx=15, pady=(15, 0))

        frame_det = ctk.CTkFrame(frame)
        frame_det.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

        cols_det = ("Archivo", "Carpeta", "Estado", "D2", "PF", "Error")
        self.tree_det = ttk.Treeview(frame_det, columns=cols_det, show="headings")
        anchos_det = {"Archivo": 250, "Carpeta": 100, "Estado": 80, "D2": 50, "PF": 50, "Error": 200}
        
        for c in cols_det:
            self.tree_det.heading(c, text=c)
            self.tree_det.column(c, width=anchos_det[c], anchor=tk.W if c in ("Archivo", "Error") else tk.CENTER)

        self.tree_det.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

    # ==================================================================
    # LÓGICA DE EJECUCIÓN
    # ==================================================================
    def _seleccionar_carpeta(self):
        carpeta = filedialog.askdirectory(initialdir=self.var_ruta.get())
        if carpeta:
            self.var_ruta.set(carpeta)

    def _iniciar_ejecucion(self):
        if self._ejecutando: return
        
        ruta = Path(self.var_ruta.get().strip())
        if not ruta.exists():
            messagebox.showerror("Error", "La ruta especificada no existe.")
            return

        self._ejecutando = True
        self.btn_ejecutar.configure(state=tk.DISABLED, text="PROCESANDO...", fg_color="#5a5a5a")
        self.barra_progreso.set(0)
        self.lbl_estado.configure(text="Arrancando motor...", text_color="white")
        
        self.txt_log.configure(state=tk.NORMAL)
        self.txt_log.delete("1.0", tk.END)
        self.txt_log.configure(state=tk.DISABLED)

        self._log("=== PIPELINE P&C INICIADO ===", "info")
        self._log(f"Ruta: {ruta}", "info")

        hilo = threading.Thread(target=self._hilo_procesador, args=(ruta, self.var_forzar.get()), daemon=True)
        hilo.start()

    def _hilo_procesador(self, ruta, forzar):
        try:
            # INSTANCIAMOS EL MOTOR NUEVO
            motor = ProcesadorPrincipal(ruta_base=ruta, forzar=forzar, on_progreso=self._recibir_progreso)
            resultado = motor.ejecutar()
            self.after(0, self._mostrar_resumen, resultado)
        except Exception as e:
            self.after(0, self._log, f"ERROR CRÍTICO: {e}", "error")
        finally:
            self.after(0, self._finalizar_interfaz)

    def _recibir_progreso(self, evento):
        self.after(0, self._procesar_evento, evento)

    def _procesar_evento(self, evento):
        tipo = evento["tipo"]
        hora = datetime.now().strftime("%H:%M:%S")

        if tipo == "inicio":
            self._total_archivos = evento["total_archivos"]
            self._procesados_actuales = 0
            self._log(f"Se detectaron {self._total_archivos} archivos.", "info")
            
        elif tipo in ["archivo_ok", "archivo_error", "archivo_omitido", "archivo_sin_datos"]:
            self._procesados_actuales += 1
            if hasattr(self, '_total_archivos') and self._total_archivos > 0:
                avance = self._procesados_actuales / self._total_archivos
                self.barra_progreso.set(avance)
                self.lbl_estado.configure(text=f"Procesando: {evento['nombre']} ({self._procesados_actuales}/{self._total_archivos})")

            if tipo == "archivo_ok":
                self._log(f"{hora} [OK] {evento['nombre']} (D2:{evento['filas_d2']} | PF:{evento['filas_pf']})", "ok")
            elif tipo == "archivo_omitido":
                self._log(f"{hora} [OMITIDO] {evento['nombre']}", "omitido")
            elif tipo == "archivo_error":
                self._log(f"{hora} [ERROR] {evento['nombre']}: {evento['error']}", "error")

        elif tipo == "parquet_escrito":
            self._log(f"{hora} [PARQUET] {evento['tipo_hoja']}.parquet guardado ({evento['filas']} filas)", "info")

    def _mostrar_resumen(self, res):
        self._log("\n" + "="*50, "info")
        self._log(f"RESULTADO: {res['estado'].upper()} - {res['stats']['total_procesados']} procesados", "exito_resumen")
        self._log(f"Tiempo total: {res['duracion']} segundos", "info")

    def _finalizar_interfaz(self):
        self._ejecutando = False
        self.btn_ejecutar.configure(state=tk.NORMAL, text="INICIAR EXTRACCIÓN P&C", fg_color="#28a745")
        self.lbl_estado.configure(text="Ejecución finalizada.", text_color="gray")
        self.barra_progreso.set(1.0)
        self._cargar_historial()
    def _log(self, texto: str, tag: str = "") -> None:
        """Agrega una línea al registro de actividad con un tag de color opcional."""
        self.txt_log.configure(state=tk.NORMAL)
        if tag:
            self.txt_log.insert(tk.END, texto + "\n", tag)
        else:
            self.txt_log.insert(tk.END, texto + "\n")
        self.txt_log.see(tk.END)
        self.txt_log.configure(state=tk.DISABLED)
    # ==================================================================
    # LÓGICA DE HISTORIAL (SQLITE)
    # ==================================================================
    def _cargar_historial(self):
        for i in self.tree_ejec.get_children(): self.tree_ejec.delete(i)
        for i in self.tree_det.get_children(): self.tree_det.delete(i)

        try:
            regs = self.historial_db.obtener_historial_reciente()
            for r in regs:
                fecha = r['fecha_inicio'][:19].replace('T', ' ')
                dur = f"{r['duracion_segundos']:.1f}s" if r['duracion_segundos'] else "-"
                self.tree_ejec.insert("", tk.END, iid=str(r['id']), values=(r['id'], fecha, r['estado'].upper(), r['archivos_exitosos'], r['archivos_error'], dur))
        except Exception as e:
            print(f"Error cargando base de datos: {e}")

    def _al_seleccionar_ejecucion(self, event):
        seleccion = self.tree_ejec.selection()
        if not seleccion: return
        ej_id = int(seleccion[0])

        for i in self.tree_det.get_children(): self.tree_det.delete(i)

        try:
            archivos = self.historial_db.obtener_detalles_archivo(ej_id)
            for a in archivos:
                self.tree_det.insert("", tk.END, values=(a['nombre_archivo'], a['carpeta_version'], a['estado'], a['filas_d2'], a['filas_pf'], a['mensaje_error']))
        except Exception:
            pass

    # ==================================================================
    # SEGURIDAD AL CERRAR
    # ==================================================================
    def _al_cerrar_ventana(self):
        if self._ejecutando:
            if not messagebox.askyesno("Ejecución en curso", "El motor está procesando datos.\n¿Seguro que deseas forzar el cierre?"):
                return
        self.destroy()

if __name__ == "__main__":
    app = VentanaPipeline()
    app.mainloop()