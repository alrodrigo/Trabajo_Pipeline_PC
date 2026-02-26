import tkinter as tk
from tkinter import ttk, filedialog, scrolledtext
import threading
from pathlib import Path
#importamos el motor de datos
from src.ajustes import FILESERVER_LOCAL, FILESERVER_PRODUCCION
from src.procesador_principal import ProcesadorPrincipal

class AplicacionGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Pipeline P&C - Codelco")
        self.root.geometry("850x650")
        self.root.resizable(False, False)

        #Variables de la interfaz
        self.ruta_actual = tk.StringVar(value=str(FILESERVER_LOCAL))
        self.forzar_var = tk.BooleanVar(value=False)

        self._crear_interfaz()
    
    def _crear_interfaz(self):
        #--- Pestaña de configuración ---
        notebook = ttk.Notebook(self.root)
        notebook.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        pestana_ejecucion = ttk.Frame(notebook)
        pestana_historial = ttk.Frame(notebook)

        notebook.add(pestana_ejecucion, text="Ejecutar Pipeline")
        notebook.add(pestana_historial, text="Historial de Ejecuciones")

        #pestaña de ejecucion

        #1 Marco de configuracion 
        frame_config = ttk.LabelFrame(pestana_ejecucion, text= "Configuracion")
        frame_config.pack(fill=tk.X, padx=10, pady=10)

        #fila de ruta
        frame_ruta = ttk.Frame(frame_config)
        frame_ruta.pack(fill=tk.X, padx=10, pady=5)

        ttk.Label(frame_ruta, text="Ruta de datos:").pack(side=tk.LEFT)
        entrada_ruta =  ttk.Entry(frame_ruta, textvariable=self.ruta_actual, width=70)
        entrada_ruta.pack(side=tk.LEFT, padx=5)
        ttk.Button(frame_ruta, text="Examinar...", command=self._seleccionar_carpeta).pack(side=tk.LEFT)

        #fila de botones rapidos
        frame_botones_ruta = ttk.Frame(frame_config)
        frame_botones_ruta.pack(fill=tk.X, padx=10, pady=0)
        ttk.Button(frame_botones_ruta, text="Usar ruta local", command=lambda: self.ruta_actual.set(str(FILESERVER_LOCAL))).pack(side=tk.LEFT, padx=(85,5))
        ttk.Button(frame_botones_ruta, text="Usar ruta produccion", command=lambda: self.ruta_actual.set(str(FILESERVER_PRODUCCION))).pack(side=tk.LEFT)

        #Checkbox Forzar
        ttk.Checkbutton(frame_config, text="Forzar reprocesamiento (ignorar fechas de modificacion)", variable=self.forzar_var).pack(anchor=tk.W, padx=10, pady=10)

        #boton ejecutar
        self.btn_ejecutar = ttk.Button(pestana_ejecucion, text="▶ Ejecutar Pipeline", command=self.iniciar_pipeline)
        self.btn_ejecutar.pack(pady=10)

        #2. Barra de progreso
        self.progreso = ttk.Progressbar(pestana_ejecucion, orient=tk.HORIZONTAL, mode='determinate')
        self.progreso.pack(fill=tk.X, padx=10, pady=10)
        
        self.lbl_estado = ttk.Label(pestana_ejecucion, text="Esperando ejecucion...")
        self.lbl_estado.pack(anchor=tk.W, padx=10)

        #3.Consola de salia

        frame_consola = ttk.LabelFrame(pestana_ejecucion, text="Registro de actividad")
        frame_consola.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        #el recuadro negro con letras verdes
        self.consola = scrolledtext.ScrolledText(frame_consola, bg="black", fg="green", font=("Consolas", 10), state=tk.DISABLED)
        self.consola.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)


    #funciones de los botones
    def _seleccionar_carpeta(self):
        carpeta= filedialog.askdirectory(initialdir=self.ruta_actual.get())
        if carpeta:
            self.ruta_actual.set(carpeta)
    
    def escribir_consola(self, mensaje):
        """Escribe texto en la pantalla negra de la interfaz"""
        self.consola.config(state=tk.NORMAL)
        self.consola.insert(tk.END, mensaje + "\n")
        self.consola.see(tk.END)#auto-scroll hacia abajo
        self.consola.config(state=tk.DISABLED)

    def iniciar_pipeline(self):
        """bloquea el boton y arranca el motor en segundo plano"""
        ruta_elegida = Path(self.ruta_actual.get())

        if not ruta_elegida.exists():
            self.escribir_consola(f"[ERROR] La ruta no existe: {ruta_elegida}")
            return
        #bloqueamos la interfaz para que el usuario no haga clicks
        self.btn_ejecutar.config(state=tk.DISABLED)
        self.progreso['value'] = 0
        self.consola.config(state=tk.NORMAL)
        self.consola.delete(1.0, tk.END)#limpiamos consola
        self.consola.config(state=tk.DISABLED)

        self.escribir_consola("---INICIANDO EXTRACCION P&C ---")

        #arrancamos el procesador en un hilo (Thread) separado
        hilo = threading.Thread(target=self._tarea_en_segundo_plano, args=(ruta_elegida,))
        hilo.start()
    def _tarea_en_segundo_plano(self, ruta):
        """Esta funcion ejecuta el motor real sin congelar la ventana """
        try:
            #iniciamos el motor recien terminado
            procesador = ProcesadorPrincipal(
                ruta_base=ruta,
                forzar=self.forzar_var.get(),
                on_progreso=self._actualizar_desde_motor #le pasamos esta funcion para que nos avise

            )
            resultado = procesador.ejecutar()

            #Resumen Final
            self.escribir_consola("\n" + "=" * 60)
            self.escribir_consola(f"RESULTADO: {resultado['estado'].upper()} -{resultado['stats']['total_procesados']} procesado(s)")
            self.escribir_consola(f"Duracion: {resultado['duracion']} segundos")

            self.lbl_estado.config(text="Ejecucion finalizada")
            self.progreso['value'] = 100 #se llena la barra final
        
        except Exception as e:
            self.escribir_consola(f"\n [ERROR CRITICO] {str(e)}")
            self.lbl_estado.config(text="Error en la ejecucion")

        finally:
            #reactivamos el boton de ejecutar
            self.btn_ejecutar.config(state=tk.NORMAL)

    def _actualizar_desde_motor(self, evento):
        """El motor llama a esta funcion cada vez que procesa un archivo"""
        tipo = evento.get("tipo")

        if tipo == "inicio":
            self.progreso['maximum'] = evento['total_archivos']   
        elif tipo == "archivo_ok":
            self.progreso['value'] += 1
            msg = f"✓ {evento['nombre']} (D2: {evento['filas_d2']} | PF: {evento['filas_pf']})"
            self.escribir_consola(msg)
        elif tipo == "archivo_omitido":
            self.progreso['value'] += 1
            self.escribir_consola(f"⚠ {evento['nombre']} - Omitido (no modificado)")
        elif tipo == "archivo_error":
            self.progreso['value'] += 1
            self.escribir_consola(f"✗ {evento['nombre']} - ERROR: {evento['error']}")
        elif tipo == "parquet_escrito":
            self.escribir_consola(f"Archivo {evento['tipo_hoja']}.parquet: {evento['filas']} filas escritas")
if __name__ == "__main__":
    ventana = tk.Tk()
    app = AplicacionGUI(ventana)
    ventana.mainloop()